mod get;
mod ping;
mod set;

use crate::cmd::get::Get;
use crate::cmd::ping::Ping;
use crate::cmd::set::Set;
use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;

pub enum Command {
    Get(Get),
    Set(Set),
    Ping(Ping),
}

impl Command {
    pub(crate) fn from_value(frame: Frame) -> crate::Result<Command> {
        unimplemented!()
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        unimplemented!()
    }
}
