use std::error::Error;

use banyan::coordinator::Coordinator;
use futures::Future;
use log::{error, info};

fn run() -> Result<(), banyan::Error> {
    let data0 = vec![
        "The waves were crashing on the shore; it was a lovely sight.",
        "There were white out conditions in the town; subsequently, the roads were impassable.",
        "They got there early, and they got really good seats.",
        "Wednesday is hump day, but has anyone asked the camel if he’s happy about it?",
        "This is a Japanese doll.",
        "I hear that Nancy is very pretty.",
        "Cats are good pets, for they are clean and are not noisy.",
        "Yeah, I think it's a good environment for learning English.",
        "The quick brown fox jumps over the lazy dog.",
        "Last Friday in three week’s time I saw a spotted striped blue worm shake hands with a legless lizard.",
    ];

    let data1 = vec![
        "Joe made the sugar cookies; Susan decorated them.",
        "Two seats were vacant.",
        "A song can make or ruin a person’s day if they let it get to them.",
        "He didn’t want to go to the dentist, yet he went anyway.",
        "Sometimes, all you need to do is completely make an ass of yourself and laugh it off to realise that life isn’t so bad after all.",
        "My Mum tries to be cool by saying that she likes all the same things that I do.",
        "Christmas is coming.",
        "I would have gotten the promotion, but my attendance wasn’t good enough.",
        "This is a Japanese doll.",
        "I was very proud of my nickname throughout high school but today- I couldn’t be any different to what my nickname was.",
    ];

    info!("Creating coordinator");
    let c = Coordinator::new()?;
    c.listen("tcp://127.0.0.1:5555")?;

    info!("Starting first batch");
    let res0 = data0
        .into_iter()
        .map(|s| c.submit(s.as_bytes()))
        .collect::<Vec<_>>();

    info!("Batch submitted");
    for future in res0 {
        let resp = future.wait()?;
        let resp = String::from_utf8_lossy(&resp);
        println!("{}", resp);
    }

    info!("Starting second batch");
    let res1 = data1
        .into_iter()
        .map(|s| c.submit(s.as_bytes()))
        .collect::<Vec<_>>();

    info!("Batch submitted");
    for future in res1 {
        let resp = future.wait()?;
        let resp = String::from_utf8_lossy(&resp);
        println!("{}", resp);
    }

    Ok(())
}

fn main() {
    env_logger::init();

    if let Err(e) = run() {
        error!("{}", e);
        if let Some(c) = e.source() {
            error!("Caused by: {}", c);
        }
        std::process::exit(1);
    }
}
