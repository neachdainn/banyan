use std::{
    error::Error,
    io::Write,
    str::{self, Utf8Error},
    thread, time,
};

use banyan::worker::{RunError, Worker};
use log::{error, info};

fn run(name: &str) -> Result<(), RunError<Utf8Error>> {
    info!("Worker \"{}\" starting", name);
    let worker = Worker::new()?;
    worker.dial_async("tcp://127.0.0.1:5555")?;

    worker.run(|mut work| {
        let msg = str::from_utf8(&work)?.to_string();
        info!("Received work: {}", msg);

        thread::sleep(time::Duration::from_secs(1));
        work.clear();
        write!(work, "{} - {}", msg.to_uppercase(), name).unwrap(); // Write to memory can't fail

        info!("Sending reply: {}", str::from_utf8(&work)?);
        Ok(work)
    })
}

fn main() {
    env_logger::init();

    let name = if let Some(name) = std::env::args().nth(1) {
        name
    } else {
        error!("Expected one argument");
        std::process::exit(1);
    };

    if let Err(e) = run(&name) {
        error!("{}", e);
        if let Some(c) = e.source() {
            error!("Caused by: {}", c);
        }
        std::process::exit(1);
    }
}
