use redis::run;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> redis::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    run(listener).await;
    Ok(())
}
