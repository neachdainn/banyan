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
use std::ffi::CString;
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
	      C: Fn(Vec<u8>) -> Result<Vec<u8>, Error>
{
	let urls = urls.as_ref();

	info!("Opening nng socket");
	let mut socket = nng::Socket::reply_open().context("Unable to open socket")?;

	// The error management story for this function is a little sad right now.
	// Rather than trying to force something to work, it is easier to simply
	// warn and ignore if the user provides no URLs.
	if urls.is_empty() {
		warn!("Worker did not connect to any coordinators. It will never receive work.");
	}

	for url in urls {
		let url = url.as_ref();

		info!("Connecting to {}", url);
		let c_url = CString::new(url).context("Unable to convert URL into c-string")?;
		socket.dial(&c_url).context("Unable to connect to URL")?;
	}

	info!("Beginning work loop");
	loop {
		debug!("Waiting for work");
		let work = socket.recv().context("Failed to receive work")?;

		debug!("Entering callback function");
		let mut result = callback_fn(work)?;

		debug!("Replying with result");
		socket.send(&mut result).context("Failed to send results")?;
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
	contexts: Vec<nng::AsyncIo>,

	/// The channel that the async IO contexts use to send data to the
	/// coordinator.
	rx: mpsc::Receiver<(usize, nng::Result<Vec<u8>>)>,

	/// The base socket.
	///
	/// This manages all of the contexts and connections from worker nodes. I
	/// am fairly confident it isn't necessary to drop this last but it is
	/// likely a good idea.
	_socket: nng::Socket,
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
		let mut socket = nng::Socket::request_open().context("Unable to open socket")?;

		for url in urls {
			let url = url.as_ref();

			info!("Listening on {}", url);
			let c_url = CString::new(url).context("Unable to convert URL into c-string")?;
			socket.listen(&c_url).context("Unable to listen to URL")?;
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

		// Start by trying to convert all of the work into `nng_msg`
		debug!("Converting work into nng messages");
		let mut work_msgs = {
			let res: Result<Vec<nng::Message>, nng::Error> =
				work.iter().map(|w| {
					let w = w.as_ref();

					let mut msg = nng::Message::with_capacity(w.len())?;
					msg.insert(&w)?;
					Ok(msg)
				}).collect();

			match res {
				Ok(work_msgs) => work_msgs.into_iter().enumerate(),
				Err(e) => return Err(BatchError { cause: e, results }),
			}
		};

		// We're going to keep track of which contexts are servicing which jobs
		// to prevent a queue of jobs waiting on specific contexts.
		let mut mapping = vec![0; self.contexts.len()];
		let mut outgoing = 0;

		// Now that all of the work is converted to messages we can start
		// sending them out. Since there are currently no pending messages we
		// can just use all of the contexts to send out messages.
		debug!("Doing preliminary work assignments");
		for ((m, (e, w)), c) in mapping.iter_mut().zip(&mut work_msgs).zip(self.contexts.iter()) {
			*m = e;
			let (aio, ctx) = (c.aio(), c.context());
			unsafe {
				aio.set_msg(w);
				ctx.send(aio);
			}

			outgoing += 1;
		}

		// From here on out, things are happening completely asynchronously.
		// The best thing to do is record the first error we see (logging all
		// others) and stop sending work requests.
		let mut error = None;

		// Now that all of our contexts are full, we need to wait for them to
		// free and queue jobs as available.
		debug!("Finishing work assignments");
		for (e, w) in work_msgs {
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

					let (aio, ctx) = (self.contexts[id].aio(), self.contexts[id].context());
					unsafe {
						aio.set_msg(w);
						ctx.send(aio);
					}

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
	fn create_async_io(socket: &mut nng::Socket, id: usize, tx: mpsc::SyncSender<(usize, nng::Result<Vec<u8>>)>) -> Result<nng::AsyncIo, Error>
	{
		let mut state = State::Send;

		Ok(nng::AsyncIo::new(socket, move |aio, ctx| {
			trace!("AsyncIO event on {} ({:?})", id, state);
			match (&state, aio.result()) {
				(&State::Send, Ok(_)) => {
					// The send went fine. Wait for a reply.
					unsafe { ctx.recv(aio); }

					state = State::Recv;
				},
				(&State::Send, Err(e)) => {
					// We failed to send the message. We need to collect it and
					// then drop it to recover the memory.
					let _ = unsafe { aio.get_msg() };
					tx.send((id, Err(e))).unwrap();

					// State stays the same
				},
				(&State::Recv, Ok(_)) => {
					// We received a valid message. Send it to the main thread.
					unsafe { tx.send((id, Ok(aio.get_msg().body().to_owned()))).unwrap(); }

					// Wait to send a message
					state = State::Send;
				}
				(&State::Recv, Err(e)) => {
					// We were unable to get the message. Inform the main thread.
					tx.send((id, Err(e))).unwrap();
					state = State::Send
				}
			}
		})?)
	}
}

/// An error occurred during batch submission
#[derive(Debug, Fail)]
#[fail(display = "Unable to prepare batch for submission")]
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

