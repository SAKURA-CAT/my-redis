mod cmd;
mod connection;
mod db;
mod frame;
mod parse;
mod server;

pub use server::run;

/// Error type for this crate
///
/// This is a type alias for `anyhow::Error`.
pub type Error = anyhow::Error;

/// A Result type for this crate
pub type Result<T> = std::result::Result<T, Error>;
