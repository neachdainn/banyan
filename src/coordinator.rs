//! Job coordinator and utilities.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::thread;

use failure::{Error, Fail, format_err, ResultExt};
use futures::Future;
use futures::sync::oneshot;
use log::{debug, error, info, trace, warn};
use nng::{Aio, Context, Protocol, Pipe, PipeEvent, Socket};

type Promise = oneshot::Sender<Result<Vec<u8>, Error>>;

/// Backend for the coordinator.
fn coordinate(commands: &mpsc::Receiver<Command>) -> Result<(), Error>
{
	let mut queue = VecDeque::new();
	let mut contexts = Vec::new();
	let mut count_offset = 0;

	loop {
		let cmd = commands.recv().unwrap_or(Command::Shutdown);

		match cmd {
			Command::Shutdown => return Ok(()),
			Command::CtxCountInc => count_offset += 1,
			Command::CtxCountDec => count_offset -= 1,
			Command::Queue(work, promise) => queue.push_back((work, promise)),
		}

		// TODO: Allocate the contexts, figure out what to do when the work is done
	}
}

/// Updates the backend with connect and disconnect events.
fn pipe_event(tx: &mpsc::SyncSender<Command>, pipe: Pipe, event: PipeEvent)
{
	use nng::options::{Options, RemAddr};

	let rem_addr_str = pipe.get_opt::<RemAddr>()
		.map(|a| format!("{:?}", a))
		.unwrap_or("?".to_string());

	match event {
		PipeEvent::AddPost => {
			debug!("New worker connected ({})", rem_addr_str);
			let _ = tx.send(Command::CtxCountInc);
		},
		PipeEvent::RemovePost => {
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

	/// Thread handle for the backend.
	jh: Option<thread::JoinHandle<Result<(), Error>>>,
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
		let mut socket = Socket::new(Protocol::Req0).context("Unable to open REQ socket")?;

		// Open up the channel to the backend function. That is all we need to do for now, since we
		// won't create any AIO objects until something connects. We need to use the sync channel in
		// order to not have issues with the Nng-rs `catch_unwind` boundary. One hundred seems
		// reasonable and not too large.
		let (tx, rx) = mpsc::sync_channel(100);

		// Now we set up the pipe notify callback so we can make sure that we don't miss any
		// events and our AIO count is accurate.
		let txc = tx.clone();
		socket.pipe_notify(move |p, ev| pipe_event(&txc, p, ev))
			.context("Unable to set the pipe notification callback")?;

		// At this point, we can finally start listening for and accepting connections.
		urls.into_iter()
			.map(|url| {
				let url = url.as_ref();
				info!("Dialing to {}", url);
				socket.listen(url).context("Unable to listen to URL")
			})
			.collect::<Result<_, _>>()?;

		debug!("Starting the backend thread");
		let jh = Some(thread::spawn(move || coordinate(&rx)));

		Ok(Coordinator {  tx, jh })
	}

	/// Queue a message on the worker nodes.
	///
	/// This function returns a future which can be polled or waited on to determine when the work
	/// is complete and retrieve the result.
	pub fn submit(&self, work: Vec<u8>) -> impl Future<Item=Vec<u8>, Error=Error>
	{
		debug!("Creating new oneshot pair and queuing work");
		let (sender, receiver) = oneshot::channel();

		let res = self.tx.send(Command::Queue(work, sender));
		if let Err(mpsc::SendError(Command::Queue(_, s))) = res {
			// This means that the backend has been shutdown (somehow?). Inform the user that
			// something went wrong.
			let _ = s.send(Err(format_err!("Backend has been shut down")));
		}

		receiver.then(|f| match f {
			Ok(Ok(r)) => Ok(r),
			Ok(Err(e)) => Err(e.into()),
			Err(_) => Err(nng::Error::from(nng::ErrorKind::Canceled).into()),
		})
	}
}
impl Drop for Coordinator
{
	fn drop(&mut self)
	{
		// If we can't send the shutdown command, it's because the thing has already been shut down.
		let _ = self.tx.send(Command::Shutdown);
		let handle = self.jh.take();

		// Join to give all promises the ability to complete.
		handle.expect("No available join handle")
			.join()
			.expect("Coordinator backend panicked")
			.expect("Coordinator backend had an error");
	}
}

/// Commands for the backend.
enum Command
{
	/// Turn the backend off.
	Shutdown,

	/// Increase the number of asynchronous contexts by one.
	CtxCountInc,

	/// Decrease the number of asynchronous contexts by one.
	CtxCountDec,

	/// Queue up a bit of work to do.
	Queue(Vec<u8>, Promise),
}
