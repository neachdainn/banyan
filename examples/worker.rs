extern crate banyan;
extern crate failure;
extern crate env_logger;

#[macro_use]
extern crate log;

use std::{thread, time};

fn main()
{
	env_logger::init();

	let args = ::std::env::args().collect::<Vec<_>>();
	let name = &args[1];

	info!("Worker \"{}\" starting", name);
	let res = banyan::start_worker(&["tcp://127.0.0.1:5555"], |work| {
		let msg = String::from_utf8(work.to_vec())?;
		info!("Received work: {}", msg);

		thread::sleep(time::Duration::from_secs(2));

		let reply = format!("{} - {}", msg.to_uppercase(), name);
		info!("Sending reply: {}", reply);

		Ok(reply.into_bytes())
	});

	if let Err(e) = res {
		error!("{}", e);
		for cause in e.iter_causes() {
			error!("Caused by: {}", cause);
		}
	}
}
