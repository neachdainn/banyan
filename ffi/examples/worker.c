#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include "banyan.h"

int callback(void *user_data, const banyan_buffer *input, banyan_buffer *output)
{
	size_t msg_len = 0;
	const char * data = (const char *) banyan_buffer_data(input, &msg_len);

	const size_t maxsz = 256;
	char buffer[maxsz];
	memset(buffer, 0, maxsz);

	size_t pos = 0;
	for(; pos < msg_len && pos < maxsz - 1; pos++) {
		buffer[pos] = toupper(data[pos]);
	}

	const char *sig_start = " - ";
	for(size_t i = 0; pos < maxsz && i < strlen(sig_start); i++, pos++) {
		buffer[pos] = sig_start[i];
	}

	char * name = (char *) user_data;
	for(size_t i = 0; pos < maxsz && i < strlen(name); i++, pos++) {
		buffer[pos] = name[i];
	}

	banyan_buffer_copy(output, buffer, pos);

	sleep(3);

	return 0;
}

int main(int argc, char** argv)
{
	if(argc != 2) {
		printf("Usage: worker <name>\n");
		return EXIT_FAILURE;
	}

	banyan_logger_start();

	banyan_worker *worker = banyan_worker_alloc(&callback, argv[1]);
	if(!worker) {
		return EXIT_FAILURE;
	}

	int dial_rc = banyan_worker_dial(worker, "tcp://127.0.0.1:5555");
	if(dial_rc != 0) {
		return EXIT_FAILURE;
	}

	int callback_rc = 0;
	int start_rc = banyan_worker_start(worker, &callback_rc);
	if(start_rc != 0 || callback_rc != 0) {
		return EXIT_FAILURE;
	}

	banyan_worker_free(worker);

	return EXIT_SUCCESS;
}

