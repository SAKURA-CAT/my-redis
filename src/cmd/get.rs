use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;

pub struct Get {
    key: String,
}

impl Get {
    pub fn from_values(frames: Vec<Frame>) -> crate::Result<Self> {
        unimplemented!()
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        unimplemented!()
    }
}
