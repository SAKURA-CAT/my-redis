mod cmd;
mod connection;
mod db;
mod frame;
mod parse;
mod server;

use crate::parse::ParseError;
use anyhow::anyhow;
pub use server::run;

/// Error type for this crate
///
/// This is a type alias for `anyhow::Error`.
pub type Error = anyhow::Error;

impl From<frame::Error> for anyhow::Error {
    fn from(value: frame::Error) -> Self {
        match value {
            frame::Error::Incomplete => anyhow!("protocol error; unexpected end of stream"),
            frame::Error::Other(err) => err,
        }
    }
}

impl From<ParseError> for anyhow::Error {
    fn from(value: ParseError) -> Self {
        match value {
            ParseError::EndOfStream => anyhow!("protocol error; unexpected end of stream"),
            ParseError::Other(err) => err,
        }
    }
}

/// A Result type for this crate
pub type Result<T> = std::result::Result<T, Error>;
