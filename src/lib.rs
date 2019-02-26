//! Banyon: A Task Distribution Library
//!
//! The purpose of this library is to provide a framework for task distribution. It takes advantage
//! of [nng][1] in order to provide scalable and resilient worker nodes.
//!
//! To use this library, a coordinator node needs to be started using the `Coordinator` type below.
//! A list of work should be passed into the coordinator which will then distribute the tasks,
//! returning the list of responses once all of the tasks have been completed. Worker nodes can be
//! started using the `start_worker` function.
//!
//! [1]: https://github.com/nanomsg/nng


pub mod worker;
pub mod coordinator;

#[cfg(feature = "ffi")]
mod ffi;
