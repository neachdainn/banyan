//! # Banyan: A task distribution library

pub mod coordinator;

pub(crate) mod error;
pub use self::error::Error;

pub mod worker;

/// Messages sent between workers and coordinators.
pub type Message = nng::Message;
