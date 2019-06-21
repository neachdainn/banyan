use std::{collections::VecDeque, panic::AssertUnwindSafe, sync::Arc};

use super::{worker::Worker, Command, Promise, HANDLER_RATIO};
use crate::error::{Error, ResultExt};
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use log::{debug, info, warn};
use nng::{Aio, Context, Message, Socket};

/// Coordinator backend.
///
/// This object manages the distribution of messages across available worker
/// nodes.
// This is a struct rather than just a function in order to make it easier to
// modify it to run an update step from multiple threads if we ever need to make
// that modification.
pub struct Backend
{
	/// The underlying NNG socket.
	socket: Socket,

	/// The channel used to receive commands.
	rx: Receiver<Command>,

	/// The sending half of the channel, used to make new workers.
	tx: Sender<Command>,

	/// Allocated workers whose ID is the position within the array.
	workers: Vec<(Arc<Worker>, Aio)>,

	/// The queue of available workers by their ID.
	available_workers: Vec<usize>,

	/// The pending work along with the promise used to fulfill the work.
	pending_work: VecDeque<(Message, Promise)>,

	/// Whether or not the backend is in the process of shutting down.
	shutting_down: bool,

	/// The maximum allowed number of running workers.
	max_running_work: usize,

	/// The number of workers that are currently running.
	running_work: usize,
}

impl Backend
{
	/// Creates a new Backend.
	pub(super) fn new(socket: Socket) -> (Backend, Sender<Command>)
	{
		let (tx, rx) = crossbeam_channel::unbounded();

		let backend = Backend {
			socket,
			rx,
			tx: tx.clone(),
			workers: Vec::new(),
			available_workers: Vec::new(),
			pending_work: VecDeque::new(),
			shutting_down: false,
			max_running_work: 0,
			running_work: 0,
		};

		(backend, tx)
	}

	/// Run the backend event loop.
	pub fn run(mut self) -> Result<(), Error>
	{
		// Only run as long as we are accepting more work, have work currently running,
		// or have work queued up.
		info!("Beginning event loop");
		while !self.shutting_down || !self.pending_work.is_empty() || self.running_work > 0 {
			self.event_loop(false)?;
		}

		Ok(())
	}

	/// Do a single step of the event loop.
	fn event_loop(&mut self, nonblocking: bool) -> Result<(), Error>
	{
		// Retrieve a command from the event queue.
		let cmd = if let Some(c) = self.recv(nonblocking) { c } else { return Ok(()) };

		// Process the incoming event.
		match cmd {
			Command::Shutdown => self.shutting_down = true,
			Command::CtxCountInc => {
				self.max_running_work = self.max_running_work.saturating_add(HANDLER_RATIO)
			},
			Command::CtxCountDec => {
				self.max_running_work = self.max_running_work.saturating_sub(HANDLER_RATIO)
			},

			Command::Queue(m, p) => {
				if !self.shutting_down {
					self.pending_work.push_back((m, p));
				}
			},

			Command::Complete(id) => {
				// First, warn if the provided ID is not a known ID.
				if id >= self.workers.len() {
					warn!("Received completion event from invalid worker (ID {})", id);
					return Ok(());
				}

				// Then, warn if we weren't expecting any work to be completed.
				self.running_work = if let Some(v) = self.running_work.checked_sub(1) {
					v
				}
				else {
					warn!("Received unexpected completion event from worker (ID {})", id);
					// Return because an idle worker isn't as bad as a faulty one.
					return Ok(());
				};

				// Finally, add the ID to the queue of available workers.
				self.available_workers.push(id);
			},
		}

		// Most commands received will either make a worker available or add a new bit
		// of work to the queue. Additionally, these can only happen in increments of
		// one, so we only need to try and distribute one bit of work.
		self.try_distribute_one()
	}

	/// Receives on the command channel, blocking if specified.
	fn recv(&self, nonblocking: bool) -> Option<Command>
	{
		// The channel should never be closed since the workers never drop their sending
		// half and the workers don't get dropped until the backend does.
		if nonblocking {
			match self.rx.try_recv() {
				Ok(c) => Some(c),
				Err(TryRecvError::Empty) => None,
				Err(TryRecvError::Disconnected) => unreachable!("Channel should be open"),
			}
		}
		else {
			debug!("Blocking on event channel");
			Some(self.rx.recv().expect("Channel should be open"))
		}
	}

	/// Try to distribute a bit of work.
	fn try_distribute_one(&mut self) -> Result<(), Error>
	{
		// Make sure that it is valid for us to have another running worker and that we
		// actually have work to distribute.
		if self.running_work >= self.max_running_work || self.pending_work.is_empty() {
			return Ok(());
		}

		// Now, find any available worker. Prefer ones that are already allocated but
		// allocate if necessary. We do this before confirming that there isn't any
		// active work because it is easy to add the worker back to a list where order
		// doesn't matter than it is to reinsert the work into the front of a queue.
		let id = self.available_workers.pop().map_or_else(|| self.allocate_worker(), Ok)?;

		// At this point, try to get any bit of work that isn't cancelled.
		let (work, promise) = if let Some((w, p)) = self.pop_work() {
			(w, p)
		}
		else {
			self.available_workers.push(id);
			return Ok(());
		};

		// Assign the worker and start the AIO operation.
		let (worker, aio) = &self.workers[id];
		let mut worker_promise = worker.promise.lock();

		assert!(worker_promise.is_none(), "Worker (ID {}) already has an active promise", id);
		*worker_promise = Some(promise);

		worker.start(aio, work);
		self.running_work += 1;

		Ok(())
	}

	/// Allocates a new worker context returning it's ID number
	fn allocate_worker(&mut self) -> Result<usize, Error>
	{
		let tx = self.tx.clone();
		let ctx = Context::new(&self.socket).context("Unable to create socket context")?;
		let id = self.workers.len();

		let worker = Arc::new(Worker::new(tx, ctx, id));
		let worker_clone = Arc::clone(&worker);

		// The current `catch_unwind` interface is a right pain in the butt. After much
		// discussion on the Rust IRC channels and repeatedly hitting my head into
		// walls, I think I've arrived at the following conclusions:
		//
		// 1. This pain is partially because {Ref}UnwindSafe wasn't in Rustc 1.0 and
		//    partially because people are overzealous about making it a pain to avoid
		//    being a general catch mechanism.
		// 2. Something being `Sync` logically means that it is also `RefUnwindSafe`
		//    but, due to backwards compatibility reasons, it is not necessarily
		//    implemented as such.
		// 3. Given the previous point, it should be perfectly valid to
		// `AssertUnwindSafe`    on any type that is `Sync`.
		//
		// Eventually this should happen in Nng-rs, but I want a working version of this
		// before I release yet another RC for v0.5.0.
		let cb = AssertUnwindSafe(move |aio, res| worker_clone.callback(aio, res));

		#[allow(clippy::redundant_closure)]
		let aio = Aio::new(move |aio, res| (*cb)(aio, res)).context("Unable to create AIO object")?;

		self.workers.push((worker, aio));
		Ok(id)
	}

	/// Retrieves the next bit of work that isn't canceled.
	fn pop_work(&mut self) -> Option<(Message, Promise)>
	{
		while let Some((m, p)) = self.pending_work.pop_front() {
			if !p.is_canceled() {
				return Some((m, p));
			}
		}

		None
	}
}
