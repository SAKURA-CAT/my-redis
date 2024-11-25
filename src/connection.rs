use crate::frame::Frame;
use bytes::BytesMut;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    buf: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            // Allocate 4KB of capacity for the buffer.
            buf: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Read a RESP value from the stream.
    pub async fn read_frame(&mut self) -> crate::Result<Frame> {
        // let bytes_read = self.stream.read_buf(&mut self.buf).await?;
        unimplemented!()
    }

    /// Write a RESP value to the stream.
    pub async fn write(&mut self, value: Frame) -> Result<(), anyhow::Error> {
        self.stream.write(value.serialize().as_bytes()).await?;
        Ok(())
    }
}
