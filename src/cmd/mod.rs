mod get;
mod ping;
mod set;
mod unknown;

use crate::cmd::get::Get;
use crate::cmd::ping::Ping;
use crate::cmd::set::Set;
use crate::cmd::unknown::Unknown;
use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parse::Parse;

pub enum Command {
    Get(Get),
    Set(Set),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    pub(crate) fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut parse = Parse::new(frame)?;
        let command_name = parse.next_string()?.to_lowercase();

        // All cmd should implement from_parse method
        // this method will parse the remaining of the frame as it expects
        let command = match command_name.as_str() {
            "get" => Command::Get(Get::from_parse(&mut parse)?),
            "set" => Command::Set(Set::from_parse(&mut parse)?),
            "ping" => Command::Ping(Ping::from_parse()),
            _ => Command::Unknown(Unknown::new(&command_name)?),
        };
        // If there are any remaining bytes in the frame, then the frame is malformed.
        parse.finish()?;

        // Return the command
        Ok(command)
    }

    /// Apply the command to the specified `Db` instance.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        use Command::*;
        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }
}
