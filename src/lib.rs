mod db;
mod resp;
mod server;

pub use server::run;

pub type Result<T> = std::result::Result<T, anyhow::Error>;
