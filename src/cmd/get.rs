use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parse::Parse;

pub struct Get {
    key: String,
}

impl Get {
    pub fn from_parse(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        Ok(Get { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let frame = if let Some(value) = db.get(&self.key) {
            Frame::Bulk(value)
        } else {
            Frame::Null
        };
        dst.write_frame(&frame).await?;
        Ok(())
    }
}
