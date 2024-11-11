#![allow(unused_imports)]

use std::io::Write;
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // unwrap: If the Result is Ok, this method returns the value. If it's Err, it panics.
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming(){
        match stream {
            Ok(mut _stream) => {
                _stream.write_all(b"+PONG\r\n").unwrap()
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
