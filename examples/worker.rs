use std::{thread, time};

use log::{error, info};

fn main()
{
	env_logger::init();

	let args = std::env::args().collect::<Vec<_>>();
	let name = &args[1];

	info!("Worker \"{}\" starting", name);
	let res = banyan::worker::start(vec!["tcp://127.0.0.1:5555"], |work| {
		let msg = String::from_utf8(work.to_vec())?;
		info!("Received work: {}", msg);

		thread::sleep(time::Duration::from_secs(1));

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
