//! Worker nodes and utilities.
use std::{error, fmt, time::Duration};

use crate::error::{Error, ResultExt};
use log::{debug, info};
use nng::{
	options::{
		transport::tcp::KeepAlive,
		Options,
		ReconnectMaxTime,
		ReconnectMinTime,
		RecvBufferSize,
	},
	Message,
	Protocol,
	Socket,
};

/// A worker node that can execute a callback upon receiving work.
#[derive(Debug)]
pub struct Worker
{
	/// The socket that will provide the worker with work.
	socket: Socket,
}

impl Worker
{
	/// Creates a new worker node.
	pub fn new() -> Result<Self, Error>
	{
		info!("Opening NNG REP0 socket");
		let socket = Socket::new(Protocol::Rep0).context("Unable to open REP0 socket")?;

		// Utilize the TCP keepalive to try to help keep things sane when there are long
		// gaps between work events.
		socket.set_opt::<KeepAlive>(true).context("Unable to set TCP keepalive")?;

		// Set NNG's queue to zero. We don't want this worker picking up work that it
		// can't immediately do. The coordinator is managing our own queue.
		socket.set_opt::<RecvBufferSize>(0).context("Unable to set receive buffer size")?;

		Ok(Worker { socket })
	}

	/// Dials to the specified URL to receive work.
	///
	/// If the dial succeeds and the resulting connection is ever lost, the
	/// worker will periodically re-attempt the dial.
	pub fn dial(&self, url: &str) -> Result<(), Error>
	{
		self.socket.dial(url).context("Failed to dial to URL")
	}

	/// Listens on the specified URL to receive work.
	pub fn listen(&self, url: &str) -> Result<(), Error>
	{
		self.socket.listen(url).context("Failed to listen to URL")
	}

	/// Asynchronously dials to the specified URL to receive work.
	///
	/// If the connection attempt fails or a connection is later disconnected,
	/// the worker will periodically re-attempt the dial.
	pub fn dial_async(&self, url: &str) -> Result<(), Error>
	{
		self.socket.dial_async(url).context("Failed to dial to URL")
	}

	/// Asynchronously listens on the specified URL to receive work.
	///
	/// If the worker fails to bind to the URL it will periodically re-attempt
	/// the bind operation.
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

	/// Begin waiting for work
	///
	/// The worker will execute the provided callback every time it receives
	/// work. The result of the callback will be sent as a reply to the
	/// coordinator node. This function will only return on an error.
	pub fn run<C, E>(self, mut callback: C) -> Result<(), RunError<E>>
	where
		C: FnMut(Message) -> Result<Message, E>,
	{
		info!("Beginning work loop");
		loop {
			debug!("Waiting for work");
			let work = self.socket.recv().context("Failed to receive work")?;

			debug!("Entering callback function");
			let result = match callback(work) {
				Ok(r) => r,
				Err(e) => return Err(RunError::Callback(e)),
			};

			debug!("Replying with result");
			self.socket.send(result).map_err(|(_, e)| e).context("Failed to send result")?;
		}
	}
}

/// An error that happened while running work.
pub enum RunError<E>
{
	/// Banyan had an internal error.
	Internal(Error),

	/// The user provided function had an error.
	Callback(E),
}

impl<E: fmt::Debug> fmt::Debug for RunError<E>
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
	{
		match self {
			RunError::Internal(e) => f.debug_tuple("RunError::Internal").field(e).finish(),
			RunError::Callback(e) => f.debug_tuple("RunError::Callback").field(e).finish(),
		}
	}
}

impl<E> fmt::Display for RunError<E>
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
	{
		write!(f, "An error occurred inside of the work loop")
	}
}

impl<E: error::Error + 'static> error::Error for RunError<E>
{
	fn description(&self) -> &str { "An error occurred inside of the work loop" }

	fn source(&self) -> Option<&(dyn error::Error + 'static)>
	{
		match self {
			RunError::Internal(e) => Some(e),
			RunError::Callback(e) => Some(e),
		}
	}
}

#[doc(hidden)]
impl<E> From<Error> for RunError<E>
{
	fn from(e: Error) -> RunError<E> { RunError::Internal(e) }
}
