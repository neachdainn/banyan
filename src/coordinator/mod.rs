//! Coordinator nodes and utilities.
use crate::error::{Error, ResultExt};
use crossbeam_channel::{Receiver, Sender};
use log::{debug, info};
use nng::{Aio, Context, Message, Pipe, PipeEvent, Protocol, Socket};

const HANDLER_RATIO: usize = 1;

pub struct Coordinator
{
}

impl Coordinator
{
	/// Creates a new coordinator node.
	///
	/// This node will listen for connecting worker nodes on the specified URLs and then will
	/// attempt to evenly distribute work among them.
	pub fn new<I, S>(urls: I) -> Result<Self, Error>
	where
		I: IntoIterator<Item = S>,
		S: AsRef<str>,
	{
		info!("Opening NNG REQ0 socket");
		let socket = Socket::new(Protocol::Req0).context("Unable to open REQ0 socket")?;

		// We will be managing our own queue, so don't let NNG interfere.
		socket.set_opt::<SendBufferSize>(0).context("Unable to set send buffer size")?;

		// Create the channels that will be used as the queues for work and idle handlers.
		let (work_tx, work_rx) = crossbeam_channel::unbounded();
		let (idle_tx, idle_rx) = crossbeam_channel::unbounded();

		// Set up the list of handlers and the pipe notify callback

		unimplemented!();
	}
}
