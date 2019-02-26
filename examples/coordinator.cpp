#include <iostream>
#include <string>
#include <vector>
#include "../src/banyan.h"

int main(int argc, char** argv)
{
	banyan_start_logger();

	std::vector<std::string> data = {
		"The waves were crashing on the shore; it was a lovely sight.",
		"There were white out conditions in the town; subsequently, the roads were impassable.",
		"They got there early, and they got really good seats.",
		"Wednesday is hump day, but has anyone asked the camel if he’s happy about it?",
		"This is a Japanese doll.",
		"I hear that Nancy is very pretty.",
		"Cats are good pets, for they are clean and are not noisy.",
		"Yeah, I think it's a good environment for learning English.",
		"The quick brown fox jumps over the lazy dog.",
		"Last Friday in three week’s time I saw a spotted striped blue worm shake hands with a legless lizard."
	};

	auto coordinator = banyan_coordinator_alloc();
	if(coordinator == nullptr) {
		return EXIT_FAILURE;
	}

	int listen_rc = banyan_coordinator_listen(coordinator, "tcp://127.0.0.1:5555");
	if(listen_rc != 0) {
		return EXIT_FAILURE;
	}

	std::vector<banyan_future*> futures;
	for(auto sentence : data) {
		auto buff = banyan_buffer_alloc();
		banyan_buffer_copy(buff, sentence.data(), sentence.size());

		auto job_future = banyan_coordinator_submit(coordinator, buff);
		if(job_future == nullptr) {
			return EXIT_FAILURE;
		}

		futures.push_back(job_future);
	}

	for(auto job_future : futures) {
		auto rep = banyan_future_wait(job_future);
		if(rep == nullptr) {
			return EXIT_FAILURE;
		}

		size_t len = 0;
		const char* msg = static_cast<const char*>(banyan_buffer_data(rep, &len));

		std::string response(msg, len);
		std::cout << response << std::endl;
	}

	return EXIT_SUCCESS;
}

