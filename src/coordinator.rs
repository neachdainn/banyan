//! Job coordinator and utilities.
use std::collections::VecDeque;
use std::sync::mpsc;
use std::thread;

use failure::{bail, Error, Fail, format_err, ResultExt};
use futures::Future;
use futures::sync::oneshot;
use log::{debug, error, info, trace, warn};
use nng::options::{Options, SendBufferSize};

type Promise = oneshot::Sender<Result<nng::Message, Error>>;

/// The number of workers to spawn per connection.
const WORKERS_PER_CONNECTION: usize = 2;

/// The size of the queue used for messages to the backend.
const BACKEND_QUEUE_SIZE: usize = 100;

/// The backend for a single coordinator.
struct Backend
{
	/// The receiving end of the commands channel.
	rx: mpsc::Receiver<Command>,

	/// The sending end of the commands channel.
	///
	/// It has to be a `SyncSender` in order to cross a `catch_unwind` boundary.
	tx: mpsc::SyncSender<Command>,

	/// The work that has not yet been assigned to a worker.
	queue: VecDeque<(nng::Message, Promise)>,

	/// The workers whose ID is their index in the list.
	workers: Vec<Worker>,

	/// The upper bound of available workers.
	max_workers: usize,

	/// The NNG socket used for all work requests.
	socket: nng::Socket,
}
impl Backend
{
	/// Create a new backend.
	fn new<S>(urls: &[S]) -> Result<Backend, Error>
		where S: AsRef<str>
	{
		info!("Opening NNG REQUEST socket");
		let mut socket = nng::Socket::new(nng::Protocol::Req0)
			.context("Unable to open REQ socket")?;

		// Keep NNG from forming its own queue. We are managing that in a way that makes sense to
		// us.
		socket.set_opt::<SendBufferSize>(1).context("Unable to set send buffer size")?;

		// Open up the channel used to receive commands. Since we have to use the sync channel, we
		// need to pick a value that is reasonable. In other words, how fast do we think that the
		// backend can handle incoming commands versus how quickly will they come. I suspect that
		// the ratio is in favor of the backend, so we'll start small.
		let (tx, rx) = mpsc::sync_channel(BACKEND_QUEUE_SIZE);

		// Now we need to set up the pipe notify functions that will inform us about the number of
		// active connections. We do this before listening to any URLs in order to make sure that we
		// don't accidentally miss any events.
		let txc = tx.clone();
		socket.pipe_notify(move |p, ev| pipe_event(&txc, p, ev))
			.context("Unable to set the pipe notification callback")?;

		// At this point, we start listening to and accepting connections.
		urls.iter()
			.map(|u| u.as_ref())
			.inspect(|url| info!("Listening to {}", url))
			.map(|url| socket.listen(url).context("Unable to listen to URL"))
			.collect::<Result<_, _>>()?;

		Ok(Backend {
			rx,
			tx,
			queue: VecDeque::new(),
			workers: Vec::new(),
			max_workers: 0,
			socket
		})
	}

	/// Creates a new sink that can be used to send commands to this backend.
	fn tx(&self) -> mpsc::SyncSender<Command>
	{
		self.tx.clone()
	}

	/// Creates a new handle to the underlying socket.
	///
	/// This handle is *not* meant to be used to send and receive messages. However, it does use its
	/// own context and no one else has access to the backend AIO objects, so it shouldn't cause
	/// issues if someone does.
	fn socket(&self) -> nng::Socket
	{
		self.socket.clone()
	}

	/// Run the backend event loop.
	fn run(mut self) -> Result<(), Error>
	{
		let mut accept_more = true;
		let mut running_work = 0;

		loop {
			// If the channel is closed, we will never receive any state updates from anything. As
			// such, we should just break out of the loop and die.
			let cmd = self.rx.recv().context("Communication channel shut down early")?;

			// If we have no running work, nothing in the queue, and we can't accept more work, just
			// break out of the loop and die.
			if !accept_more && running_work + self.queue.len() == 0 {
				return Ok(());
			}

			// Otherwise, process the command.
			match cmd {
				Command::Shutdown => accept_more = false,
				Command::CtxCountInc => self.max_workers = self.max_workers.saturating_add(WORKERS_PER_CONNECTION),
				Command::CtxCountDec => self.max_workers = self.max_workers.saturating_sub(WORKERS_PER_CONNECTION),

				Command::Queue(work, promise) => {
					// We always want to pull the most recent bit of work, so we'll just put the
					// newest one on the back of the queue.
					if accept_more {
						self.queue.push_back((work, promise));
					}
				},

				Command::Complete(id, rep) => {
					if id > self.workers.len() {
						warn!("Received result from invalid worker ID ({})", id);
						continue;
					}

					running_work = match running_work.checked_sub(1) {
						Some(v) => v,
						None => {
							warn!("Received unexpected but valid results from worker ID ({})", id);
							0
						}
					};

					let promise = match self.workers[id].promise.take() {
						Some(p) => p,
						None => {
							warn!("Worker ID ({}) has no promise to fulfill", id);
							continue;
						}
					};

					let _ = promise.send(rep);
				}
			}

			// Every single command either makes a single worker available or adds a single bit of
			// work. As such, we only need to try and assign work once for every iteration.
			if self.try_assign_work() {
				running_work += 1;
			}
		}
	}

	/// Attempts to assign a work to a worker, returning `true` if an assigment happened.
	fn try_assign_work(&mut self) -> bool
	{
		// If a promise has already been canceled, there is no reason to assign it to a worker. As
		// such, we can clear out promises until we find one that isn't canceled. If there is no
		// available work, return early to avoid making unnecessary workers.
		match self.queue.iter().position(|(_, p)| !p.is_canceled()) {
			Some(0) => {},
			Some(n) => { self.queue.drain(0..n); },
			None => {
				self.queue.clear();
				return false;
			}
		}

		if let Some(id) = self.next_available_worker() {
			let (work, promise) = self.queue.pop_front().unwrap();
			let worker = &mut self.workers[id];

			// Try to send the work. If something goes wrong, forward it to the consumer via the
			// promise.
			if let Err((_, e)) = worker.ctx.send(&worker.aio, work) {
				let _ = promise.send(Err(e.into()));
				false
			} else {
				worker.promise = Some(promise);
				true
			}
		} else { false }
	}

	/// Returns the ID of the next available worker.
	///
	/// If necessary and allowed, this function will also start a new worker.
	fn next_available_worker(&mut self) -> Option<usize>
	{
		// First, find the slice over which we're able to search for free workers.
		let search_space = {
			let ub = std::cmp::min(self.workers.len(), self.max_workers);
			&mut self.workers[0..ub]
		};

		// Now, given that search space, is there a free worker?
		let search = search_space.into_iter().position(|w| w.promise.is_none());
		if search.is_some() {
			return search;
		}

		// We couldn't find a free worker, so we should create one if available.
		let id = self.workers.len();
		if self.max_workers > id {
			match self.create_worker(id) {
				Ok(w) => {
					self.workers.push(w);
					Some(id)
				},
				Err(e) => {
					warn!("Failed to create a new worker context: {}", e);
					None
				}
			}
		} else { None }
	}

	/// Creates a new worker object.
	fn create_worker(&mut self, id: usize) -> Result<Worker, nng::Error>
	{
		let mut state = State::Send;
		let ctx = nng::Context::new(&self.socket)?;
		let ctxc = ctx.clone();
		let tx = self.tx.clone();

		let aio = nng::Aio::with_callback(move |aio| {
			trace!("Aio event on {} ({:?})", id, state);

			// First, get the AIO result. This *should* always have a value but it is better to
			// inform the user than to deal with the weird panic dance.
			let result = match aio.result() {
				Some(r) => r,
				None => {
					let _ = tx.send(Command::Complete(id, Err(format_err!("AIO has no result"))));
					state = State::Send;
					return;
				},
			};

			// Now we can act based on our current state and whether or not the operation was
			// successful.
			match (&state, result) {
				// The send went fine.
				(&State::Send, Ok(_)) => {
					// This also should never happen but, again, it is better to inform than to play
					// the panic dance.
					if let Err(e) = ctxc.recv(aio) {
						let e = e.context("Failed to start the recv operation");
						let _ = tx.send(Command::Complete(id, Err(e.into())));
					}
					else {
						state = State::Recv;
					}
				},

				// The send operation went poorly.
				(&State::Send, Err(e)) => {
					let _ = tx.send(Command::Complete(id, Err(e.into())));
				},

				// The receive operation went find.
				(&State::Recv, Ok(_)) => {
					let msg = aio.get_msg().ok_or(format_err!("AIO had no message available"));
					let _ = tx.send(Command::Complete(id, msg));

					state = State::Send;
				},

				// The receive operation went poorly.
				(&State::Recv, Err(e)) => {
					let _ = tx.send(Command::Complete(id, Err(e.into())));
					state = State::Send;
				}
			}
		})?;

		Ok(Worker { promise: None, aio, ctx })
	}
}

/// Updates the backend with connect and disconnect events.
fn pipe_event(tx: &mpsc::SyncSender<Command>, pipe: nng::Pipe, event: nng::PipeEvent)
{
	use nng::options::{Options, RemAddr};

	let rem_addr_str = pipe.get_opt::<RemAddr>()
		.map(|a| format!("{:?}", a))
		.unwrap_or("?".to_string());

	match event {
		nng::PipeEvent::AddPost => {
			debug!("New worker connected ({})", rem_addr_str);
			let _ = tx.send(Command::CtxCountInc);
		},
		nng::PipeEvent::RemovePost => {
			debug!("Worker disconnected ({})", rem_addr_str);
			let _ = tx.send(Command::CtxCountDec);
		},
		_ => {},
	}
}

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
	/// Channel for communication with the backend.
	tx: mpsc::SyncSender<Command>,

	/// An additional handle to the backend's socket.
	///
	/// This is useful for being able to manage URLs even after the backend has been started.
	socket: nng::Socket,

	/// Thread handle for the backend.
	jh: Option<thread::JoinHandle<Result<(), Error>>>,
}
impl Coordinator
{
	/// Creates a new coordinator node.
	///
	/// The node will listen on the specified URLs for connections from worker nodes and will then
	/// distribute work fairly among them.
	pub fn new<S>(urls: &[S]) -> Result<Self, Error>
		where S: AsRef<str>
	{
		let backend = Backend::new(urls).context("Unable to start backend start failed")?;
		let tx = backend.tx();
		let socket = backend.socket();

		debug!("Starting backend thread");
		let jh = Some(thread::spawn(move || {
			let res = backend.run();
			if let Err(e) = &res {
				error!("Error in Banyan backend: {}", e);
				e.iter_causes().for_each(|c| error!("Caused by: {}", c));
			}

			res
		}));

		Ok(Coordinator {  tx, socket, jh })
	}

	/// Begins listening on a new URL.
	pub fn listen(&mut self, url: &str) -> Result<(), Error>
	{
		self.socket.listen(url).context("Unable to listen to URL")?;
		Ok(())
	}

	/// Queue a message on the worker nodes.
	///
	/// This function returns a future which can be polled or waited on to determine when the work
	/// is complete and retrieve the result.
	pub fn submit<W>(&self, work: W) -> Response
		where W: Into<nng::Message>
	{
		let work = work.into();

		trace!("Creating new oneshot pair and queuing work");
		let (sender, receiver) = oneshot::channel();

		let res = self.tx.send(Command::Queue(work, sender));
		if let Err(mpsc::SendError(Command::Queue(_, s))) = res {
			// This means that the backend has been shutdown (somehow?). Inform the user that
			// something went wrong.
			let _ = s.send(Err(format_err!("Backend has been shut down")));
		}

		Response { inner: receiver }
	}

	/// Shut down the coordinator, blocking until all current futures have a result.
	///
	/// It is not necessary to call this function to be certain that all pending futures will
	/// complete. Even if the Coordinator is dropped without explicitely shutting down, all futures
	/// will be fulfilled.
	pub fn shutdown(mut self) -> Result<(), Error>
	{
		// If we can't send the shutdown command, it's because the backend has already died.
		let _ = self.tx.send(Command::Shutdown);

		// The backend won't join until all currently pending work items have been completed.
		// Additionally, since we took `self` there is no way for more items to be added to the
		// queue. If the backend panicked, we also want to panic, otherwise return any error the
		// backend may have had.
		self.jh.take().expect("No join handle").join().expect("The backend has panicked")
	}
}
impl Drop for Coordinator
{
	fn drop(&mut self)
	{
		// Let things play out how they will. I am fairly certain all pending futures will still be
		// completed.
		let _ = self.tx.send(Command::Shutdown);
	}
}

/// The promise of a response from a Banyan worker node.
pub struct Response
{
	inner: oneshot::Receiver<Result<nng::Message, Error>>,
}

impl Future for Response
{
	type Item = Vec<u8>;
	type Error = Error;

	fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error>
	{
		use futures::Async;

		match self.inner.poll() {
			Ok(Async::Ready(Ok(r))) => Ok(Async::Ready(r.to_vec())),
			Ok(Async::Ready(Err(e))) => Err(e.into()),
			Ok(Async::NotReady) => Ok(Async::NotReady),
			Err(_) => Err(nng::Error::from(nng::ErrorKind::Canceled).into())
		}
	}
}

/// An asynchronous I/O context
struct Worker
{
	/// The promise that this context is currently working to fulfill.
	promise: Option<Promise>,

	/// The asynchronous task space.
	aio: nng::Aio,

	/// The socket context.
	ctx: nng::Context,
}

/// The state of a Worker object.
#[derive(Debug)]
enum State
{
	/// Attempting to send a request to a connection.
	Send,

	/// Waiting to receive a response from a connection.
	Recv,
}

/// Commands for the backend.
enum Command
{
	/// Turn the backend off cleanly.
	Shutdown,

	/// Increase the number of asynchronous contexts by one.
	CtxCountInc,

	/// Decrease the number of asynchronous contexts by one.
	CtxCountDec,

	/// Queue up a bit of work to do.
	Queue(nng::Message, Promise),

	/// Mark a bit of work as done and the context as free.
	Complete(usize, Result<nng::Message, Error>),
}
