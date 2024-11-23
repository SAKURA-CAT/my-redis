
mod resp;

use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // unwrap: If the Result is Ok, this method returns the value. If it's Err, it panics.
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) =>{
                println!("accepted new connection");
                tokio::spawn(async move {
                    handle_conn(stream).await
                });
            }
            Err(e) => {
                println!("error: {}", e);
                }
        }
    }
}


async fn handle_conn(mut stream: TcpStream) {
    // Every connection will be created a new RespParser
    let parser = resp::RespParser::new(stream);
    loop {

    }
}