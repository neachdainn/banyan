use super::{Command, Promise};
use crate::error::{Error, ResultExt};
use crossbeam_channel::Sender;
use log::error;
use nng::{Aio, AioResult, Context, Message};
use parking_lot::Mutex;

/// An asynchronous I/O context.
pub(super) struct Worker
{
	/// The channel used to send update information to the backend.
	pub tx: Sender<Command>,

	/// The promise to be fulfilled on the next incoming message.
	pub promise: Mutex<Option<Promise>>,

	/// The socket context used to manage messages.
	pub ctx: Context,

	/// The ID of this worker.
	pub id: usize,
}

impl Worker
{
	/// Creates a new worker with the given sender, context, and ID.
	pub fn new(tx: Sender<Command>, ctx: Context, id: usize) -> Worker
	{
		Worker { tx, promise: Mutex::new(None), ctx, id }
	}

	/// Starts a send operation on this worker.
	pub fn start(&self, aio: &Aio, work: Message)
	{
		if let Err(e) = self.ctx.send(aio, work).map_err(|(_, e)| e) {
			self.report(Err(e).context("Unable to begin send operation"));
		}
	}

	/// The AIO callback function for this worker.
	pub fn callback(&self, aio: Aio, res: AioResult)
	{
		match res {
			AioResult::SendOk => {
				if let Err(e) = self.ctx.recv(&aio) {
					self.report(Err(e).context("Unable to begin receive operation"));
				}
			},
			AioResult::SendErr(_, e) => self.report(Err(e).context("Failed to send message")),

			AioResult::RecvOk(m) => self.report(Ok(m)),
			AioResult::RecvErr(e) => self.report(Err(e).context("Failed to receive message")),

			AioResult::SleepOk | AioResult::SleepErr(_) => unreachable!("Worker never sleeps"),
		}
	}

	/// Attempts to send the result back through the promise.
	fn report(&self, res: Result<Message, Error>)
	{
		let promise = self.promise.lock().take();

		if let Some(p) = promise {
			// If we can't send the error, it's because the receiving end has been dropped.
			// We don't care about that case.
			let _ = p.send(res);
		}
		else {
			// If we don't have a promise, things are going bad and are about to spiral
			// out-of-control... But there is nothing we can do about it short of aborting
			// the program. We don't do that since much of the other work might still be
			// getting done correctly.
			error!("AIO context attempting to update without a promise");
		}

		// This should never fail. If it does, we don't care because we're about to go
		// out-of-scope.
		let _ = self.tx.send(Command::Complete(self.id));
	}
}
