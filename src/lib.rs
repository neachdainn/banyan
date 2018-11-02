//! Banyon: A Task Distribution Library
//!
//! The purpose of this library is to provide a framework for task
//! distribution. It takes advantage of [nng][1] in order to provide scalable
//! and resilient worker nodes.
//!
//! To use this library, a coordinator node needs to be started using the
//! `Coordinator` type below. A list of work should be passed into the
//! coordinator which will then distribute the tasks, returning the list of
//! responses once all of the tasks have been completed. Worker nodes can be
//! started using the `start_worker` function.
//!
//! [1]: https://github.com/nanomsg/nng

#[macro_use]
extern crate log;

#[macro_use]
extern crate failure;

extern crate nng;

use std::sync::mpsc;
use nng::{Socket, Protocol};
use nng::aio::{Aio, Context};
use failure::{Error, ResultExt};

/// Creates a worker node that executes the callback function up receiving work.
///
/// This function requires a callback that will be run every time the worker
/// node receives work. The raw message is passed to the callback function and
/// the returned data is sent directly back to the coordinator.
///
/// If multiple URLs are specified the worker will split its work fairly
/// between all connected coordinators.
pub fn start_worker<U, A, C>(urls: U, callback_fn: C) -> Result<(), Error>
	where U: AsRef<[A]>, A: AsRef<str>,
	      C: Fn(&[u8]) -> Result<Vec<u8>, Error>
{
	let urls = urls.as_ref();

	info!("Opening nng socket");
	let mut socket = Socket::new(Protocol::Rep0).context("Unable to open socket")?;

	ensure!(!urls.is_empty(), "Not dialing to any URLs. The worker will never receive work");

	for url in urls {
		let url = url.as_ref();

		info!("Connecting to {}", url);
		socket.dial(url).context("Unable to connect to URL")?;
	}

	info!("Beginning work loop");
	loop {
		debug!("Waiting for work");
		let work = socket.recv().context("Failed to receive work")?;

		debug!("Entering callback function");
		let result = callback_fn(&work).context("Callback function resulted in an error")?;

		debug!("Replying with result");
		socket.send(result[..].into())
			.map_err(|(_, e)| e)
			.context("Failed to send results")?;
	}
}

/// A coordinator node
///
/// This node will organize and distribute the work between all available
/// worker nodes. Currently, this only exposes a batch processing mode where a
/// batch of tasks are given to the coordinator and it will return the results
/// once all of the work has been processed. In future versions there may be
/// more operating modes available.
pub struct Coordinator
{
	/// The asynchronous IO contexts.
	contexts: Vec<(Context, Aio)>,

	/// The channel that the async IO contexts use to send data to the
	/// coordinator.
	rx: mpsc::Receiver<(usize, nng::Result<Vec<u8>>)>,

	/// The base socket.
	///
	/// This manages all of the contexts and connections from worker nodes. I
	/// am fairly confident it isn't necessary to drop this last but it is
	/// likely a good idea.
	_socket: Socket,
}
impl Coordinator
{
	/// Creates a new coordinator node.
	///
	/// The node will listen on the specified URLs for connections from worker
	/// nodes and then distribute work fairly among all workers. The user is
	/// also required to supply the expected number of workers for the
	/// following reasons.
	///
	/// 1. We can only have so many file descriptors open at a given time.
	///    Setting the expected number of workers too high can potentially
	///    overrun this number.
	/// 2. This number also serves as a queue for the work distribution. If a
	///    new worker node joins after a batch has been started, then it
	///    will have to wait for this number of jobs to be completed by
	///    other workers before the new node is given a task.
	/// 3. This is the number of jobs that can be run in parallel. If it is set
	///    too low then you will not be taking advantage of all of the
	///    available nodes.
	///
	/// All in all, this means to try and keep the expected number of workers
	/// close to the actual number but that it is better to overshoot than to
	/// undershoot.
	pub fn new<U, A>(urls: U, expected_num_workers: usize) -> Result<Coordinator, Error>
		where U: AsRef<[A]>, A: AsRef<str>
	{
		let urls = urls.as_ref();

		ensure!(!urls.is_empty(), "Not listening on any URLs. Coordinator cannot function");
		ensure!(expected_num_workers >= 1, "No expected workers. Coordinator cannot function");

		info!("Opening socket");
		let mut socket = Socket::new(Protocol::Req0).context("Unable to open socket")?;

		for url in urls {
			let url = url.as_ref();

			info!("Listening on {}", url);
			socket.listen(url).context("Unable to listen to URL")?;
		}

		// Create the MPSC queue that will allow the async IO thread to send
		// messages to us. I am not certain what the correct size for this
		// would be, but it seems reasonable to add a slot for every single
		// asynchronous IO context. I suspect they won't play fair, but it
		// should work well enough.
		let (tx, rx) = mpsc::sync_channel(expected_num_workers);

		// Create the asynchronous IO objects and the socket contexts
		let contexts =
			(0..expected_num_workers)
			.map(|id| Coordinator::create_async_io(&mut socket, id, tx.clone()))
			.collect::<Result<Vec<_>, _>>()
			.context("Unable to create asynchronous IO mechanisms")?;

		Ok(Coordinator { _socket: socket, contexts, rx })
	}

	/// Send out a batch of work and collect the results.
	///
	/// The returned vector of results is in the same order as the input vector
	/// of work. That is to say, the first item of work in the work vector
	/// corresponds to the first result in the result vector.
	///
	/// Keep in mind that this function isn't terribly memory friendly. The
	/// work vector, the intermediate work vector, and the results vector all
	/// exist at the same time.
	pub fn submit_batch<W, R>(&mut self, work: W) -> Result<Vec<Vec<u8>>, BatchError>
		where W: AsRef<[R]>, R: AsRef<[u8]>
	{
		// Convert the argument to the type we really want to work with.
		let work = work.as_ref();

		//Then begin the actual function.
		let mut results = vec![None; work.len()];

		// We're going to keep track of which contexts are servicing which jobs
		// to prevent a queue of jobs waiting on specific contexts.
		let mut mapping = vec![0; self.contexts.len()];
		let mut outgoing = 0;

		// Create the iterator outside of loops so we can keep track of which
		// message is next.
		let mut work_iter = work.iter().map(|w| w.as_ref()).enumerate();

		// Since there are currently no pending messages we can just use all of
		// the contexts to send out messages.
		debug!("Doing preliminary work assignments");
		for ((mapping, (wid, work)), c) in mapping.iter_mut().zip(&mut work_iter).zip(self.contexts.iter()) {
			let (ctx, aio) = c;

			*mapping = wid;
			aio.send(ctx, work.into()).expect("Aio should have no pending events");
			outgoing += 1;
		}

		// From here on out, things are happening completely asynchronously.
		// The best thing to do is record the first error we see (logging all
		// others) and stop sending work requests.
		let mut error = None;

		// Now that all of our contexts are full, we need to wait for them to
		// free and queue jobs as available.
		debug!("Finishing work assignments");
		for (e, w) in work_iter {
			let (id, res) = self.rx.recv().unwrap();
			outgoing -= 1;

			// Check to see if this was an error. If it was, don't send out any
			// more work. If it wasn't, mark as advanced and keep going.
			match res {
				Ok(r) => {
					trace!("Received results {} from {}", mapping[id], id);

					let work_id = mapping[id];
					results[work_id] = Some(r);
					mapping[id] = e;

					let (ref ctx, ref aio) = self.contexts[id];
					aio.send(&ctx, w.into()).expect("Aio should have no pending events");
					outgoing += 1;
				},
				Err(e) => {
					error!("Work request on {} failed: {}", id, e);
					error = Some(e);
					break;
				}
			}
		}

		// At this point, all of the work has been sent out and we just need to
		// wait for responses.
		for _ in 0..outgoing {
			let (id, res) = self.rx.recv().unwrap();

			match res {
				Ok(r) => {
					trace!("Received results {} from {}", mapping[id], id);

					let work_id = mapping[id];
					results[work_id] = Some(r);
				}
				Err(e) => {
					error!("Work request on {} failed: {}", id, e);
					error = Some(e);
				}
			}
		}

		// Now, we check to see if there was an error. If there was, return the
		// partial results. If not, convert from the partial results format to
		// the completed results format.
		match error {
			Some(e) => Err(BatchError { cause: e, results }),
			None => Ok(results.into_iter().map(|r| r.unwrap()).collect()),
		}
	}

	/// Creates an asynchronous context pair
	fn create_async_io(
		socket: &mut Socket,
		id: usize,
		tx: mpsc::SyncSender<(usize, nng::Result<Vec<u8>>)>
	) -> Result<(Context, Aio), Error>
	{
		let mut state = State::Send;
		let ctx = Context::new(socket).context("Unable to create context")?; // Ha!
		let ctx_clone = ctx.clone();

		let aio = Aio::with_callback(move |aio| {
			trace!("Aio event on {} ({:?})", id, state);
			match (&state, aio.result().expect("Should have results in callback")) {
				(&State::Send, Ok(_)) => {
					// The send went fine. Wait for a reply.
					aio.recv(&ctx).expect("Shouldn't have an operation queued");

					state = State::Recv;
				},
				(&State::Send, Err(e)) => tx.send((id, Err(e))).unwrap(),
				(&State::Recv, Ok(_)) => {
					// We received a valid message. Send it to the main thread.
					let msg = aio.get_msg().expect("Successful receive shoul have a message");
					tx.send((id, Ok(msg[..].into()))).unwrap();

					// Wait to send a message
					state = State::Send;
				}
				(&State::Recv, Err(e)) => {
					// We were unable to get the message. Inform the main thread.
					tx.send((id, Err(e))).unwrap();
					state = State::Send
				}
			}
		}).context("Unable to create Aio object")?;

		Ok((ctx_clone, aio))
	}
}

/// An error occurred during batch submission
#[derive(Debug, Fail)]
#[fail(display = "Unable to complete batch")]
pub struct BatchError
{
	#[cause]
	cause: nng::Error,

	/// The results as available.
	pub results: Vec<Option<Vec<u8>>>,
}

/// The state of the `AsyncIo` in the background thread
#[derive(Debug)]
enum State
{
	/// Waiting for results from sending a message.
	Send,

	/// Waiting to receive a message.
	Recv,
}

