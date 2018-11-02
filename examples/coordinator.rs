extern crate banyan;
extern crate failure;
extern crate env_logger;

#[macro_use]
extern crate log;

fn run() -> Result<(), failure::Error>
{
	let data0 = vec![
		Vec::from("The waves were crashing on the shore; it was a lovely sight."),
		Vec::from("There were white out conditions in the town; subsequently, the roads were impassable."),
		Vec::from("They got there early, and they got really good seats."),
		Vec::from("Wednesday is hump day, but has anyone asked the camel if he’s happy about it?"),
		Vec::from("This is a Japanese doll."),
		Vec::from("I hear that Nancy is very pretty."),
		Vec::from("Cats are good pets, for they are clean and are not noisy."),
		Vec::from("Yeah, I think it's a good environment for learning English."),
		Vec::from("The quick brown fox jumps over the lazy dog."),
		Vec::from("Last Friday in three week’s time I saw a spotted striped blue worm shake hands with a legless lizard."),
	];

	let data1 = vec![
		Vec::from("Joe made the sugar cookies; Susan decorated them."),
		Vec::from("Two seats were vacant."),
		Vec::from("A song can make or ruin a person’s day if they let it get to them."),
		Vec::from("He didn’t want to go to the dentist, yet he went anyway."),
		Vec::from("Sometimes, all you need to do is completely make an ass of yourself and laugh it off to realise that life isn’t so bad after all."),
		Vec::from("My Mum tries to be cool by saying that she likes all the same things that I do."),
		Vec::from("Christmas is coming."),
		Vec::from("I would have gotten the promotion, but my attendance wasn’t good enough."),
		Vec::from("This is a Japanese doll."),
		Vec::from("I was very proud of my nickname throughout high school but today- I couldn’t be any different to what my nickname was."),
	];

	info!("Creating coordinator");
	let mut c = banyan::Coordinator::new(&["tcp://127.0.0.1:5555"], 4)?;

	info!("Starting first batch");
	let res0 = c.submit_batch(data0)?;

	info!("Batch complete");
	for (i, r) in res0.into_iter().enumerate() {
		let s = String::from_utf8(r).unwrap();
		info!("{}: {}", i, s);
	}

	info!("Starting second batch");
	let res1 = c.submit_batch(data1)?;

	info!("Batch complete");
	for (i, r) in res1.into_iter().enumerate() {
		let s = String::from_utf8(r).unwrap();
		info!("{}: {}", i, s);
	}

	Ok(())
}

fn main()
{
	env_logger::init();

	if let Err(e) = run() {
		error!("{}", e);
		for cause in e.iter_causes() {
			error!("Caused by: {}", cause);
		}
	}
}
