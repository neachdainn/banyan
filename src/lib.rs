//! # Banyan: A task distribution library

// Set up the Clippy lints
#![deny(bare_trait_objects)]
#![deny(missing_debug_implementations)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![deny(clippy::wrong_pub_self_convention)]
#![warn(clippy::nursery)]
#![warn(clippy::pedantic)]
#![warn(clippy::cargo)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::decimal_literal_representation)]
#![warn(clippy::print_stdout)]
#![warn(clippy::unimplemented)]
#![warn(clippy::use_debug)]
#![allow(clippy::use_self)]
#![allow(clippy::replace_consts)]

pub mod coordinator;

pub(crate) mod error;
pub use self::error::Error;

pub mod worker;

/// Messages sent between workers and coordinators.
pub type Message = nng::Message;
