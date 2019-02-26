#ifndef BANYAN_H
#define BANYAN_H

#ifdef __cplusplus
extern "C" {
#endif

// A coordinator object that can be used to distribute tasks among worker nodes.
typedef struct banyan_coordinator banyan_coordinator;

// A buffer used for sending messages between workers and coordinators.
typedef struct banyan_buffer banyan_buffer;

// Callback function used for the `banyan_worker`.
//
// The first pointer is user-supplied data. The second is a pointer to the received work
// information. The third it the output buffer that will be sent back to the coordinator. Any return
// value except for `0` will cause the worker to stop.
typedef int (*banyan_worker_callback)(void *, const banyan_buffer *, banyan_buffer *);

// A worker object that is used to perform tasks assigned by a coordinator.
typedef struct banyan_worker banyan_worker;

// Represents a future value.
typedef struct banyan_future banyan_future;

// Starts the Banyan environment logger.
void banyan_logger_start(void);

// Creates a new coordinator object.
//
// This function returns `NULL` on any error. Check the Banyan logger for more details.
banyan_coordinator * banyan_coordinator_alloc(void);

// Frees a Banyan coordinator.
void banyan_coordinator_free(banyan_coordinator *coord);

// Has the Banyan coordinator listen on the specified NNG address.
//
// This function returns non-zero on error. Check the Banyan logger for more details.
int banyan_coordinator_listen(banyan_coordinator *coord, const char *url);

// Submits the provided work to the Banyan coordinator.
//
// This function takes ownership of the `banyan_buffer` and it should not be used afterwards. This
// function returns `NULL` on any error. Check the Banyan logger for more details.
banyan_future * banyan_coordinator_submit(banyan_coordinator *coord, banyan_buffer *buf);

// Allocates a new Banyan worker node.
//
// This function returns `NULL` on any error. Check the Banyan logger for more details.
banyan_worker * banyan_worker_alloc(banyan_worker_callback cb, void *user_data);

// Frees a Banyan worker node.
void banyan_worker_free(banyan_worker *worker);

// Adds a URL to which the worker will dial.
//
// The worker will not actually dial the URL until it is started. This function returns a non-zero
// integer on error. See the Banyan logger for more details.
int banyan_worker_dial(banyan_worker *worker, const char *url);

// Begins a Banyan worker.
//
// Except in the case of errors, this function will never return. However, it does not invalidate
// the `banyan_worker` in any way, so the object can be started multiple times. However, the worker
// *cannot* be freed while it is running.
//
// The return code from the callback function is returned via the argument. The function itself
// returns a non-zero integer in the case of a Banyan internal error.
int banyan_worker_start(banyan_worker *worker, int *ret_code);

// Waits for the Banyan future to complete.
//
// This function takes ownership of the `banyan_future` which should not be used again. The returned
// buffer is the reply from the worker and the function returns `NULL` upon error.
banyan_buffer * banyan_future_wait(banyan_future * future);

// Abandons the Banyan future, not waiting for it to complete.
//
// This function takes ownership of the `banyan_future` which should not be used again.
void banyan_future_abandon(banyan_future * future);

// Allocates a new Banyan buffer.
banyan_buffer * banyan_buffer_alloc(void);

// Frees an allocated Banyan buffer.
void banyan_buffer_free(banyan_buffer *buff);

// Copies the provided data into the buffer.
//
// This totally replaces the data in the buffer. If the length is zero or if the input data is a
// `NULL` pointer, then the buffer is simply cleared.
void banyan_buffer_copy(banyan_buffer *buff, const void *data, size_t len);

// Returns the pointer to the buffered data as well as the length of the buffered data.
//
// This returns `NULL` upon error. Check the Banyan logger for more details.
const void * banyan_buffer_data(const banyan_buffer *buff, size_t *len);

#ifdef __cplusplus
}
#endif

#endif
