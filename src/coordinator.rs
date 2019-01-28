//! Job coordinator and utilities.
use std::sync::{Arc, Mutex, Weak};
use std::collections::VecDeque;

use failure::{Error, Fail, format_err, ResultExt};
use futures::Future;
use futures::sync::oneshot;
use log::{debug, error, info, trace, warn};
use nng::{Aio, Context, Protocol, Pipe, PipeEvent, Socket};

type Promise = oneshot::Sender<Result<Vec<u8>, Error>>;

/// A coordinator node.
///
/// This node will organize and distribute the work between all available worker nodes. Each message
/// is sent to a reply node and a message is expected to be returned. If multiple reply nodes are
/// connected, then the coordinator will attempt to be fair in its distribution of work.
///
/// The Coordinator makes heavy use of the NNG "Request" sockets, meaning that the reply nodes do
/// not necessarily need to be running the worker function.
pub struct Coordinator
{
	/// The shared portion.
	inner: Arc<Mutex<Inner>>,
}
impl Coordinator
{
	/// Creates a new coordinator node.
	///
	/// The node will listen on the specified URLs for connections from worker nodes and will then
	/// distribute work fairly among them.
	pub fn new<U, S>(urls: U) -> Result<Self, Error>
		where U: IntoIterator<Item = S>,
		      S: AsRef<str>
	{
		info!("Opening NNG REQUEST socket");
		let socket = Socket::new(Protocol::Req0).context("Unable to open REQ socket")?;

		// Move the socket into the Inner object, clone the object, and then lock the mutex for the
		// duration of the function so we don't have to keep relocking it.
		let inner = Arc::new(Mutex::new(Inner::new(socket)));
		{
			let socket = &mut inner.lock().unwrap().socket;

			// We want to set the pipe notify callback before listening to anything to make sure that we
			// don't miss any pipe events and that our connection count is accurate.
			let i = inner.clone();
			socket.pipe_notify(move |p, ev| Inner::pipe_event(&i, p, ev))
				.context("Unable to set pipe notify callback")?;

			// Now that we're counting our connections, we can start accepting them.
			urls.into_iter()
				.map(|url| {
					let url = url.as_ref();
					info!("Dialing to {}", url);
					socket.listen(url).context("Unable to listend to URL")
				})
				.collect::<Result<_, _>>()?;
		}

		// The pipe notify callback will allocate all of the contexts and AIO objects for us, so we
		// don't need to do anything else here.
		Ok(Coordinator { inner })
	}

	/// Queue a message on the worker nodes.
	///
	/// This function returns a future which can be polled or waited on to determine when the work
	/// is complete and retrieve the result.
	pub fn submit(&self, work: Vec<u8>) -> impl Future<Item=Vec<u8>, Error=Error>
	{
		info!("Submitting work to workers");
		self.inner.lock().unwrap().submit(work)
	}
}

/// The shared portion of the coordinator.
struct Inner
{
	/// The socket that is used to send out all of the work requests.
	socket: Socket,

	/// Work that is currently pending.
	work: VecDeque<(Vec<u8>, Promise)>,

	/// The list of available socket contexts.
	available: Vec<WorkerContext>,

	/// The list of socket contexts that currently have a request pending.
	busy: Vec<WorkerContext>,

	/// The number of free worker contexts that need to be removed.
	///
	/// This happens when a worker node disconnects from the coordinator. Since we might find
	/// ourselves in a position where a number of workers have disconnected but all of the contexts
	/// are busy.
	overflow: usize,

	/// The next available worker context id.
	next_context_id: usize,
}
impl Inner
{
	/// Create the inner data using the provided socket.
	fn new(socket: Socket) -> Self
	{
		Inner {
			socket,
			work: VecDeque::new(),
			available: Vec::new(),
			busy: Vec::new(),
			overflow: 0,
			next_context_id: 0,
		}
	}

	/// Process a pipe notification event.
	fn pipe_event(inner: &Arc<Mutex<Self>>, pipe: Pipe, event: PipeEvent)
	{
		use nng::options::{Options, RemAddr};

		let rem_addr_str =
			pipe.get_opt::<RemAddr>()
				.map(|addr| format!("{:?}", addr))
				.unwrap_or("?".to_string());

		match event {
			PipeEvent::AddPost => {
				debug!("New worker connected ({})", rem_addr_str);
				// If we can't create the worker for any reason, it will be much better if we just
				// report the error and then move on. Things might be slower but they'll be fine
				// otherwise.
				if let Err(e) = Inner::create_worker_context(inner) {
					error!("Creating worker context failed");
					e.iter_chain().for_each(|c| error!("Caused by: {}", c));
				}
				inner.lock().unwrap().cycle_all_work();
			},
			PipeEvent::RemovePost => {
				debug!("Worker disconnected ({})", rem_addr_str);
				// The overflow wrapping around is strictly worse than saturating. In either case,
				// the user has bigger issues so we're not going to do anything about it, just make
				// sure we don't panic.
				let mut lock_guard = inner.lock().unwrap();
				lock_guard.overflow = match lock_guard.overflow.checked_add(1) {
					Some(o) => o,
					None => {
						warn!("Too many workers disconnected at once.");
						lock_guard.overflow
					},
				};
				lock_guard.cleanup_overflow();
			},
			_ => {},
		}
	}

	/// Submit work for processing.
	fn submit(&mut self, work: Vec<u8>) -> impl Future<Item=Vec<u8>, Error=Error>
	{
		debug!("Creating new oneshot pair and queuing work");
		let (sender, receiver) = oneshot::channel();
		self.work.push_back((work, sender));

		debug!("Cycling all work");
		self.cycle_all_work();

		receiver.then(|f| match f {
			Ok(Ok(r)) => Ok(r),
			Ok(Err(e)) => Err(e.into()),
			Err(_) => Err(nng::Error::from(nng::ErrorKind::Canceled).into()),
		})
	}

	/// Assigns as much work as possible to all available worker contexts.
	fn cycle_all_work(&mut self)
	{
		// Filter out all of the canceled work to avoid the overhead of dispatching work that will
		// never be received.
		self.work.retain(|(_, p)| !p.is_canceled());

		// The drain method always removes the specified amount and it will panic if you provide too
		// large of a range. This means that we need to check ahead of time which one will be
		// larger.
		let min_count = std::cmp::min(self.work.len(), self.available.len());

		// Pair up the free worker contexts with the queued work.
		let newly_busy_workers = self.available.drain(0..min_count)
			.zip(self.work.drain(0..min_count))
			.map(|(ctx, (work, promise))| {
				ctx.send_request(work, promise);
				ctx
			});

		// Move all of the newly busy workers into the busy list.
		self.busy.extend(newly_busy_workers);
	}

	/// Attempts to assign more work to the worker context.
	fn cycle_one(&mut self, id: usize)
	{
		// First, find the context in the busy list.
		let idx = match self.busy.iter().position(|w| w.id == id) {
			Some(i) => i,
			None => {
				warn!("Worker #{} ⇒ Attempting to cycle but not in busy list", id);
				return;
			}
		};

		// If we have any work, we don't need to remove it from the list, we just need to get more
		// work. If there is no work, we need to put it back in the available list.
		if let Some((work, promise)) = self.work.pop_front() {
			self.busy[idx].send_request(work, promise);
		}
		else {
			self.available.push(self.busy.swap_remove(idx));
		}
	}

	/// Remove as many of the overflow workers as possible.
	fn cleanup_overflow(&mut self)
	{
		let min_count = std::cmp::min(self.available.len(), self.overflow);
		self.available.drain(0..min_count);
		self.overflow = 0;
	}

	/// Add a new worker to the available set.
	///
	/// This requires access to the `Arc` since it is going to be _weakly_ cloned into the AIO
	/// callback.
	fn create_worker_context(inner: &Arc<Mutex<Self>>) -> Result<(), Error>
	{
		let mut lock_guard = inner.lock().unwrap();

		// If we have a worker overflow, we don't need to worry about creating a new worker context,
		// we can just reduce the overflow count by one.
		if lock_guard.overflow > 0 {
			lock_guard.overflow -= 1;
			return Ok(());
		}

		let id = lock_guard.next_context_id;
		lock_guard.next_context_id += 1;

		let worker = WorkerContext::new(Arc::downgrade(inner), &lock_guard.socket, id)
			.context("Unable to create new worker context")?;
		lock_guard.available.push(worker);

		Ok(())
	}
}

/// An context used to submit work to be completed asynchronously.
struct WorkerContext
{
	/// The asynchronous IO objects.
	aio: Aio,

	/// The socket context used to manage work state.
	ctx: Context,

	/// The state of the worker context.
	state: Arc<Mutex<State>>,

	/// The ID of this worker context.
	id: usize, 
}
impl WorkerContext
{
	/// Create a new worker context that utilizes the provided Inner type.
	fn new(inner: Weak<Mutex<Inner>>, socket: &Socket, id: usize) -> Result<Self, Error>
	{
		let ctx = Context::new(socket).context("Unable to create socket context")?;
		let state = Arc::new(Mutex::new(State { receiving: false, promise: None }));

		let info = CallbackInfo {
			inner: inner,
			state: state.clone(),
			ctx: ctx.clone(),
			id
		};

		let aio = Aio::with_callback(move |aio| {
			// If things go wrong, the best thing that we can really do at this point is really just
			// to try and log it and then move on. This is better than trying to panic into C code.
			if let Err(e) = WorkerContext::callback(aio, &info) {
				error!("Error in AIO callback");
				e.iter_chain().for_each(|c| error!("Caused by: {}", c));
			}
		}).context("Unable to create AIO object")?;

		Ok(WorkerContext { aio, ctx, state, id })
	}

	/// Send out the given work that is associated with the given promise.
	fn send_request(&self, work: Vec<u8>, promise: Promise)
	{
		// If sending this errors out, we don't really have a good way to communicate that to the
		// user of the library. As such, we're just going to send it via the promise. If the promise
		// is already close, we also don't care.
		if let Err((_, e)) = self.ctx.send(&self.aio, work[..].into()) {
			let _ = promise.send(Err(e.into()));
		}
		else {
			*self.state.lock().unwrap() = State { receiving: false, promise: Some(promise) };
		}
	}

	/// Process the AIO event.
	fn callback(aio: &Aio, info: &CallbackInfo) -> Result<(), Error>
	{
		trace!("WorkerContext #{} ⇒ AIO event", info.id);

		// We can't report anything to the user until we have a lock on the state. The first thing
		// we need to do is try and get that lock.
		let mut state = info.state.lock()
			.map_err(|_| nng::Error::from(nng::ErrorKind::IncorrectState))
			.context("Unable to lock the worker context's state")?;

		// From this point forward, all issues can be reported to the user instead of returned. The
		// next thing we need to do is get the result from the AIO object to supply to the state
		// machine.
		let res = match aio.result() {
			Some(r) => r,
			None => return state.report_err(format_err!("Result not available on AIO object")),
		};

		// We can now enter the state machine.
		trace!("Worker #{} ⇒ ({}, {:?})", info.id, state.receiving, res);
		match (state.receiving, res) {
			(false, Ok(_)) => {
				// The message was successfully sent. Wait for the response.
				if let Err(e) = info.ctx.recv(aio) {
					// Forward any sending error to the user.
					state.report_err(e.context("Could not receive response").into())
				}
				else {
					state.receiving = true;
					Ok(())
				}
			},

			(false, Err(e)) => {
				// We failed to send the message. Let the user know.
				state.receiving = false;
				state.report_err(e.context("Unable to send request").into())
			},

			(true, Ok(_)) => {
				// We successfully received a message. Send it to the user.
				state.receiving = false;

				// Safely pull the message from the AIO object. Again, inform the user if something
				// went wrong.
				let msg = match aio.get_msg() {
					Some(m) => m,
					None => return state.report_err(format_err!("No message available on AIO")),
				};

				// Send the message to the user.
				state.report_ok(msg[..].into())?;

				// Finally, we need to queue up more work for this worker context. We also need to
				// make absolutely sure that we unlock the state mutex otherwise we will get a
				// deadlock. If we can't upgrade the weak pointer, it means that the coordinator is
				// gone and there will be no more work to acquire.
				std::mem::drop(state);
				if let Some(inner_mtx) = info.inner.upgrade() {
					inner_mtx.lock()
						.map(|mut inner| inner.cycle_one(info.id))
						.map_err(|_| format_err!("Inner mutex is poisoned"))
				}
				else {
					Ok(())
				}
			},

			(true, Err(e)) => {
				// We failed to receive the message. Let the user know.
				state.receiving = false;
				state.report_err(e.context("Unable to receive response").into())
			}
		}
	}
}

/// Worker context state information.
struct State
{
	/// Whether or the worker context is sending or receiving.
	receiving: bool,

	/// The promise for the responder to fulfill.
	promise: Option<Promise>,
}
impl State
{
	/// Report the given error back to the user via the promise.
	fn report_err(&mut self, e: Error) -> Result<(), Error>
	{
		match self.promise.take() {
			Some(p) => {
				// After sending _anything_ to the user, the state is always sending.
				self.receiving = false;
				let _ = p.send(Err(e));
				Ok(())
			},
			None => {
				Err(format_err!("There was no promise to fulfill"))
			},
		}
	}

	/// Send the valid message to the user via the promise.
	fn report_ok(&mut self, m: Vec<u8>) -> Result<(), Error>
	{
		match self.promise.take() {
			Some(p) => {
				self.receiving = false;
				let _ = p.send(Ok(m));
				Ok(())
			},
			None => {
				Err(format_err!("There was no promise to fulfill"))
			}
		}
	}
}

/// Information needed for the WorkerContext callback.
struct CallbackInfo
{
	inner: Weak<Mutex<Inner>>,
	state: Arc<Mutex<State>>,
	ctx: Context,
	id: usize
}
