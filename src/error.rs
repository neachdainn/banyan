//! Error types and aliases.
use std::{error, fmt};

#[derive(Debug)]
pub struct Error
{
	context: &'static str,
	cause: nng::Error,
}

impl error::Error for Error
{
	fn source(&self) -> Option<&(dyn error::Error + 'static)>
	{
		Some(&self.cause)
	}
}

impl fmt::Display for Error
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
	{
		write!(f, "{}", self.context)
	}
}

pub trait ResultExt<A>
{
	fn context(self, ctx: &'static str) -> Result<A, Error>;
}

impl<A> ResultExt<A> for Result<A, nng::Error>
{
	fn context(self, ctx: &'static str) -> Result<A, Error>
	{
		self.map_err(|e| Error { context: ctx, cause: e })
	}
}
