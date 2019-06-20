//! Coordinator nodes and utilities.
use std::{thread::JoinHandle, time::Duration};

use crate::error::{Error, ResultExt};
use crossbeam_channel::Sender;
use futures::{sync::oneshot, Future};
use log::{error, info};
use nng::{
	options::{
		transport::tcp::KeepAlive,
		Options,
		ReconnectMaxTime,
		ReconnectMinTime,
		SendBufferSize,
	},
	Message,
	PipeEvent,
	Protocol,
	Socket,
};

mod backend;
mod worker;

/// The resolving end of the returned futures.
type Promise = oneshot::Sender<Result<Message, Error>>;

const HANDLER_RATIO: usize = 1;

/// A coordinator node that distributes work among all connected worker nodes.
///
/// Dropping the Coordinator will not block and will not prevent the pending
/// futures from being fulfilled. If one wishes to wait until all futures are
/// fulfilled, either wait on the futures themselves or use the
/// `Coordinator::shutdown` function.
#[derive(Debug)]
pub struct Coordinator
{
	/// The underlying NNG socket.
	socket: Socket,

	/// The channel used to communicate with the backend.
	tx: Sender<Command>,

	/// The join handle to the backend thread.
	jh: Option<JoinHandle<Result<(), Error>>>,
}

impl Coordinator
{
	/// Creates a new coordinator node.
	pub fn new() -> Result<Self, Error>
	{
		info!("Opening NNG REQ0 socket");
		let socket = Socket::new(Protocol::Req0).context("Unable to open REQ0 socket")?;

		// Utilize the TCP keepalive to try and keep things sane when there are long
		// gaps between work events.
		socket.set_opt::<KeepAlive>(true).context("Unable to set TCP keepalive")?;

		// We will be managing our own queue, so don't let NNG interfere.
		socket.set_opt::<SendBufferSize>(0).context("Unable to set send buffer size")?;

		// Now, set up the backend that will do the majority of the labor.
		let (backend, tx) = backend::Backend::new(socket.clone());

		// Finally, set up the pipe notify function.
		let txc = tx.clone();
		socket
			.pipe_notify(move |_, ev| match ev {
				PipeEvent::AddPost => {
					let _ = txc.send(Command::CtxCountInc);
				},
				PipeEvent::RemovePost => {
					let _ = txc.send(Command::CtxCountDec);
				},
				_ => {},
			})
			.context("Unable to set up pipe notify function")?;

		// Boot up the backend thread and we're good to go.
		let jh = std::thread::spawn(move || {
			let res = backend.run();

			if let Err(e) = &res {
				error!("Error in backend: {}", e);
			}

			res
		});

		Ok(Coordinator { socket, tx, jh: Some(jh) })
	}

	/// Dials to the specified URL to distribute work.
	///
	/// If the dial succeeds and the resulting connection is ever lost, the
	/// coordinator will periodically re-attempt the dial.
	pub fn dial(&self, url: &str) -> Result<(), Error>
	{
		self.socket.dial(url).context("Failed to dial to URL")
	}

	/// Listens on the specified URL to distribute work.
	pub fn listen(&self, url: &str) -> Result<(), Error>
	{
		self.socket.listen(url).context("Failed to listen to URL")
	}

	/// Asynchronously dials to the specified URL to distribute work.
	///
	/// If the connection attempt fails or a connection is later disconnected,
	/// the coordinator will periodically re-attempt the dial.
	pub fn dial_async(&self, url: &str) -> Result<(), Error>
	{
		self.socket.dial_async(url).context("Failed to dial to URL")
	}

	/// Asynchronously listens on the specified URL to distribute work.
	///
	/// If the coordinator fails to bind to the URL it will periodically
	/// re-attempt the bind operation.
	pub fn listen_async(&self, url: &str) -> Result<(), Error>
	{
		self.socket.listen_async(url).context("Failed to listen to URL")
	}

	/// Sets the maximum amount of time between reconnection attempts.
	pub fn set_reconn_max_time(&self, d: Option<Duration>) -> Result<(), Error>
	{
		self.socket.set_opt::<ReconnectMaxTime>(d).context("Unable to set reconnect max time")
	}

	/// Sets the minimum amount of time between reconnection attempts.
	pub fn set_reconn_min_time(&self, d: Option<Duration>) -> Result<(), Error>
	{
		self.socket.set_opt::<ReconnectMinTime>(d).context("Unable to set reconnect min time")
	}

	/// Queue a message on the worker nodes.
	///
	/// This function returns a future which can be polled or waited on to
	/// determine when the work is complete and retrieve the result.
	pub fn submit<M: Into<Message>>(&self, work: M) -> Response
	{
		let work = work.into();

		let (sender, receiver) = oneshot::channel();

		// If we fail to send the command to the backend, then the sender will be
		// dropped. This means that the receiver end will get a "canceled" error.
		let _ = self.tx.send(Command::Queue(work, sender));

		Response { inner: receiver }
	}

	/// Shut down the coordinator, blocking until all current futures have a
	/// result.
	///
	/// It is not necessary to call this function to be certain that all pending
	/// futures will complete. Even if the Coordinator is dropped without
	/// explicitely shutting down, all futures will be fulfilled.
	pub fn shutdown(mut self) -> Result<(), Error>
	{
		// If we can't send the shutdown command, it's because the backend has already
		// died.
		let _ = self.tx.send(Command::Shutdown);

		// The backend won't join until all currently pending work items have been
		// completed. Additionally, since we took `self` there is no way for more items
		// to be added to the queue. If the backend panicked, we also want to panic,
		// otherwise return any error the backend may have had.
		self.jh.take().expect("No join handle").join().expect("The backend has panicked")
	}
}

impl Drop for Coordinator
{
	fn drop(&mut self)
	{
		// Let the backend finish what it's doing but don't wait for it.
		let _ = self.tx.send(Command::Shutdown);
	}
}

/// The types of commands that can be sent to the backend.
enum Command
{
	/// Turn off the backend as soon as possible.
	Shutdown,

	/// Increase the number of available workers by the handler ratio.
	CtxCountInc,

	/// Decrease the number of available workers by the handler ratio.
	CtxCountDec,

	/// Add some work to the queue.
	Queue(Message, Promise),

	/// Mark a worker with the given ID as available.
	Complete(usize),
}

/// The promise of a response from a Banyan worker node.
#[derive(Debug)]
pub struct Response
{
	inner: oneshot::Receiver<Result<Message, Error>>,
}

impl Future for Response
{
	type Error = Error;
	type Item = Message;

	fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error>
	{
		use futures::Async;

		match self.inner.poll() {
			Ok(Async::Ready(Ok(r))) => Ok(Async::Ready(r)),
			Ok(Async::Ready(Err(e))) => Err(e),
			Ok(Async::NotReady) => Ok(Async::NotReady),
			Err(_) => Err(nng::Error::Canceled).context("Work task was canceled"),
		}
	}
}
