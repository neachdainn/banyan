#![allow(non_camel_case_types)]
//! A C-style Interface for Banyan.
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

use crate::coordinator::Coordinator;
use failure::{Error, format_err};
use futures::Future;
use log::{error, warn};

// TODO: Catch panics at the FFI boundary..
// None of these function do it and I justify this to myself by saying that the only things that can
// panic in this file are memory allocations, which are a weird "will probably never happen" zone
// anyway.

/// A C-style wrapper around a Coordinator.
struct banyan_coordinator(Coordinator);

/// A C-style wrapper around `Vec<u8>`.
struct banyan_buffer(Vec<u8>);

/// The callback type for a `banyan_worker`.
///
/// Any return code except for zero will cause the worker to stop.
type callback = extern "C" fn(*mut c_void, *const banyan_buffer, *mut banyan_buffer) -> c_int;

/// A worker object.
// I'm using a new type instead of just a function because I don't want to have to deal with
// closures and vectors of strings.
struct banyan_worker
{
	urls: Vec<String>,
	user_data: *mut c_void,
	callback: callback,
}

/// A C-style wrapper around a Banyan future.
// TODO: Remove double box.
// It actually isn't necessary for the Banyan future type to be a boxed trait object. If the closure
// inside of `Coordinator::submit` is turned into a proper function, it will be possible to
// reference the exact type it is returning. Combine this with an "internal submit" function, we can
// remove the box here.
struct banyan_future(Box<dyn Future<Item=Vec<u8>, Error=Error>>);

/// Starts the Rust environment logger.
#[no_mangle]
extern "C" fn banyan_start_logger()
{
	if let Err(e) = env_logger::try_init() {
		warn!("Attempted to start logger multiple times: {}", e);
	}
}

/// Creates a new `banyan_coordinator` object.
#[no_mangle]
extern "C" fn banyan_coordinator_alloc() -> *mut banyan_coordinator
{
	let coordinator = match Coordinator::new::<String>(&[]) {
		Ok(c) => c,
		Err(e) => {
			error!("Failed to create Coordinator object in `banyan_coordinator_alloc`: {}", e); 
			return ptr::null_mut();
		}
	};

	Box::into_raw(Box::new(banyan_coordinator(coordinator)))
}

/// Deallocates the `banyan_coordinator`.
///
/// This does not block and all existing `banyan_future` objects remain valid.
#[no_mangle]
extern "C" fn banyan_coordinator_free(coord: *mut banyan_coordinator)
{
	if coord.is_null() {
		warn!("Null pointer passed to `banyan_coordinator_free`");
		return;
	}

	unsafe { Box::from_raw(coord); }
}

/// Has the coordinator listen on the specified URL.
#[no_mangle]
extern "C" fn banyan_coordinator_listen(coord: *mut banyan_coordinator, url: *const c_char) -> c_int
{
	if coord.is_null() {
		warn!("Received null coordinator in `banyan_coordinator_listen`");
		return -1;
	}

	if url.is_null() {
		warn!("Received null URL in `banyan_coordinator_listen`");
		return -2;
	}

	unsafe {
		let url = match CStr::from_ptr(url).to_str() {
			Ok(s) => s,
			Err(e) => {
				warn!("URL contains invalid UTF-8 in `banyan_coordinator_listen`: {}", e);
				return -3;
			}
		};

		match (*coord).0.listen(url) {
			Ok(_) => 0,
			Err(e) => {
				error!("Unable to listen to URL in `banyan_coordinator_listen`: {}", e);
				-4
			}
		}
	}
}

/// Submits the provided work to the coordinator.
///
/// This function takes ownership of the `banyan_buffer` and it should not be used afterwards.
#[no_mangle]
extern "C" fn banyan_coordinator_submit(
	coord: *mut banyan_coordinator,
	buffer: *mut banyan_buffer
) -> *mut banyan_future {
	if coord.is_null() {
		warn!("Received null coordinator in `banyan_coordinator_submit`");
		return ptr::null_mut();
	}

	if buffer.is_null() {
		warn!("Received null buffer in `banyan_coordinator_submit`");
		return ptr::null_mut();
	}

	unsafe {
		// We need the `Box::from_raw` because we're taking ownership of the memory
		let f = (*coord).0.submit(Box::from_raw(buffer).0);
		Box::into_raw(Box::new(banyan_future(Box::new(f))))
	}
}

/// Creates a new Banyan worker node.
#[no_mangle]
extern "C" fn banyan_worker_alloc(cb: Option<callback>, ud: *mut c_void) -> *mut banyan_worker
{
	let cb = match cb {
		Some(cb) => cb,
		None => {
			warn!("No callback function provided to `banyan_worker_alloc`");
			return ptr::null_mut();
		}
	};

	Box::into_raw(Box::new(banyan_worker {
		urls: Vec::new(),
		user_data: ud,
		callback: cb
	}))
}

/// Frees a Banyan worker node.
#[no_mangle]
extern "C" fn banyan_worker_free(worker: *mut banyan_worker)
{
	if worker.is_null() {
		warn!("Null pointer passed to `banyan_worker_free`");
		return;
	}

	unsafe { Box::from_raw(worker); }
}

/// Adds a URL to which the worker will dial.
#[no_mangle]
extern "C" fn banyan_worker_dial(worker: *mut banyan_worker, url: *const c_char) -> c_int
{
	if worker.is_null() {
		warn!("Received null worker in `banyan_worker_dial`");
		return -1;
	}

	if url.is_null() {
		warn!("Received null URL in `banyan_worker_dial`");
		return -2;
	}

	unsafe {
		let url = match CStr::from_ptr(url).to_str() {
			Ok(s) => s,
			Err(e) => {
				warn!("URL contains invalid UTF-8 in `banyan_worker_dial`: {}", e);
				return -3;
			}
		};

		(*worker).urls.push(url.to_string());
		0
	}
}

/// Begins a worker.
///
/// Except in the case of errors, this function will never return. However, it does not invalidate
/// the `banyan_worker` in any way, so a `banyan_worker` can be started multiple times. However, the
/// worker *cannot* be freed while it is running.
///
/// The return code of the callback function will be provided via the argument.
#[no_mangle]
extern "C" fn banyan_worker_start(worker: *mut banyan_worker, ret_code: *mut c_int) -> c_int
{
	if worker.is_null() {
		warn!("Received null worker in `banyan_worker_start`");
		return -1;
	}

	let cb = move |msg: &[u8]| {
		// C is not taking ownership of these, so we don't need to box them.
		let input = banyan_buffer(msg.to_owned());
		let mut output = banyan_buffer(Vec::new());

		unsafe {
			*ret_code = ((*worker).callback)((*worker).user_data, &input as _, &mut output as _);

			if *ret_code != 0 {
				Err(format_err!("Non-zero return code: {}", *ret_code))
			} else {
				Ok(output.0)
			}
		}
	};

	unsafe {
		match crate::worker::start((*worker).urls.iter(), cb) {
			Ok(_) => 0,
			Err(e) => {
				error!("Worker failed: {}", e);
				-2
			}
		}
	}
}

/// Waits for the `banyan_future` to complete.
///
/// This function takes ownership of the future and it should not be used afterwards.
#[no_mangle]
extern "C" fn banyan_future_wait(future: *mut banyan_future) -> *mut banyan_buffer
{
	if future.is_null() {
		warn!("Received null future in `banyan_future_wait`");
		return ptr::null_mut();
	}

	unsafe {
		let future = Box::from_raw(future);
		match future.0.wait() {
			Ok(b) => Box::into_raw(Box::new(banyan_buffer(b))),
			Err(e) => {
				warn!("Error in `banyan_future` object: {}", e);
				ptr::null_mut()
			}
		}
	}
}

/// Abandons a `banyan_future`.
///
/// This function takes ownership of the future and it should not be used afterwards.
#[no_mangle]
extern "C" fn banyan_future_abandon(future: *mut banyan_future)
{
	if future.is_null() {
		warn!("Received null future in `banyan_future_abandon`");
		return;
	}

	unsafe { Box::from_raw(future); }
}

/// Creates a new `banyan_buffer` object.
#[no_mangle]
extern "C" fn banyan_buffer_alloc() -> *mut banyan_buffer
{
	Box::into_raw(Box::new(banyan_buffer(Vec::new())))
}

/// Frees an allocated `banyan_buffer` object.
#[no_mangle]
extern "C" fn banyan_buffer_free(buffer: *mut banyan_buffer)
{
	if buffer.is_null() {
		warn!("Received null buffer in `banyan_buffer_free");
		return;
	}

	unsafe {
		let _ = Box::from_raw(buffer);
	}
}

/// Copies the pointed-to data into the buffer, clearing it if the pointer is `null`.
#[no_mangle]
extern "C" fn banyan_buffer_copy(buffer: *mut banyan_buffer, data: *const c_void, len: usize)
{
	if buffer.is_null() {
		warn!("Received null buffer in `banyan_buffer_copy`");
		return;
	}

	unsafe {
		(*buffer).0.clear();

		if data.is_null() || len == 0 {
			return;
		}

		(*buffer).0.reserve(len);
		ptr::copy(data as _, (*buffer).0.as_mut_ptr(), len);
		(*buffer).0.set_len(len);
	}
}

/// Returns a pointer to the data owned by the buffer.
#[no_mangle]
extern "C" fn banyan_buffer_data(buffer: *const banyan_buffer, len: *mut usize) -> *const c_void
{
	if buffer.is_null() {
		warn!("Received null buffer in `banyan_buffer_data`");
		return ptr::null();
	}

	if len.is_null() {
		warn!("Received null length pointer in `banyan_buffer_data`");
		return ptr::null();
	}

	unsafe {
		*len = (*buffer).0.len();
		(*buffer).0.as_ptr() as _
	}
}
