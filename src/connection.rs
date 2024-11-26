use crate::frame::Frame;
use bytes::{Buf, BytesMut};
use std::io;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(stream),
            // Allocate 4KB of capacity for the buffer.
            buf: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Read a RESP value from the stream.
    ///
    /// This function will read from the stream until a full RESP line is read.
    /// There may be additional data left in the buffer after the call to this
    pub(crate) async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buf).await? {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("connection reset by peer"))
                };
            }
        }
    }

    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use crate::frame::Error::Incomplete;
        let mut buf = Cursor::new(&self.buf[..]);
        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;
                buf.set_position(0);
                let frame = Frame::parse(&mut buf)?;
                self.buf.advance(len);
                Ok(Some(frame))
            }
            Err(Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        self.stream.write_all(frame.serialize().as_bytes()).await?;
        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.stream.flush().await
    }
}
