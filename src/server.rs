use crate::cmd::Command;
use crate::connection::Connection;
use crate::db::{Db, DbGuard};
use tokio::net::{TcpListener, TcpStream};

/// Server listener state. Created in the [run] function.
/// It is used to accept new connections, and some other server-wide tasks,
/// e.g. limit the number of connections.
#[derive(Debug)]
struct Server {
    listener: TcpListener,
    db_guard: DbGuard,
}

#[derive(Debug)]
struct Handler {
    db: Db,
    connection: Connection,
}

pub async fn run(listener: TcpListener) {
    let mut server = Server {
        listener,
        db_guard: DbGuard::new(),
    };

    server.run().await;
}

impl Server {
    async fn run(&mut self) {
        loop {
            let stream = self.accept().await;
            let mut handler = Handler {
                db: self.db_guard.db(),
                connection: Connection::new(stream),
            };
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    eprintln!("Error: {:?}", err);
                }
            });
        }
    }

    async fn accept(&mut self) -> TcpStream {
        // TODO handle error
        self.listener.accept().await.unwrap().0
    }
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
        loop {
            let maybe_frame = self.connection.read_frame().await?;
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };
            let cmd = Command::from_frame(frame);
            cmd?.apply(&self.db, &mut self.connection).await?;
        }
    }
}
