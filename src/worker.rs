//! Worker nodes and utilities.
use std::time::Duration;

use failure::{Error, ResultExt};
use nng::{
	Protocol,
	Socket,
	options::{Options, ReconnectMaxTime, ReconnectMinTime, transport::tcp::KeepAlive}
};
use log::{info, debug};

/// Creates a worker node that executes the callback function upon receiving work.
///
/// This function requires a callback that will be run every time the worker node receives work. The
/// raw message is passed to the callback function and the returned data is sent directly back to
/// the requester. If multiple URLS are specified, the worker will split its work fairly between all
/// connected requesters.
///
/// This function is largely just a wrapper around an NNG "Reply" socket, which means that the
/// requester does not necessarily have to be a Coordinator node.
pub fn start<U, S, C>(urls: U, mut callback: C) -> Result<(), Error>
	where U: IntoIterator<Item = S>,
	      S: AsRef<str>,
	      C: FnMut(&[u8]) -> Result<Vec<u8>, Error>
{
	info!("Opening NNG REPLY socket");
	let mut socket = Socket::new(Protocol::Rep0).context("Unable to open REP socket")?;

	// The worker is liable to be sitting for a while as coordinators come and go. As such, we
	// probably want to enable the TCP keepalive setting to make it easier to detect when a
	// coordinator goes down.
	socket.set_opt::<KeepAlive>(true).context("Unable to set TCP keepalive")?;

	// We also want the workers to be fairly responsive to a coordinator coming back online.
	socket.set_opt::<ReconnectMinTime>(Some(Duration::from_secs(0)))
		.context("Failed to set reconnect min time")?;
	socket.set_opt::<ReconnectMaxTime>(Some(Duration::from_secs(30)))
		.context("Failed to set reconnect max time")?;

	// Setting the socket to non-blocking mode for the dial operations will allow us to start the
	// workers before the coordinator.
	socket.set_nonblocking(true);
	urls.into_iter()
		.map(|url| {
			let url = url.as_ref();
			info!("Dialing to {}", url);
			socket.dial(url).context("Unable to dial to URL")
		})
		.collect::<Result<_, _>>()?;

	// Now that we've dialed out, go back to blocking mode to make managing the work easier.
	socket.set_nonblocking(false);

	info!("Beginning work loop");
	loop {
		debug!("Waiting for work");
		let work = socket.recv().context("Failed to receive work")?;

		debug!("Entering callback function");
		let result = callback(&work).context("Callback function resulted in an error")?;

		debug!("Reply with result");
		socket.send(result[..].into())
			.map_err(|(_, e)| e)
			.context("Failed to send results")?;
	}
}
