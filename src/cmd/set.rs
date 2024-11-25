pub struct Set {
    key: String,
    value: String,
    expire: Option<i64>,
}

impl Set {
    pub fn from_values() -> crate::Result<Self> {
        unimplemented!()
    }
}
