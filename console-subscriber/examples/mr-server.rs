use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    console_subscriber::init();

    connect::connect().await;
}

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

mod connect {
    use tokio::net::{TcpListener};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use super::{Arc, Mutex};
    use super::process::process;
    use super::HashMap;

    pub(crate) async fn connect() {
        // Bind the listener to the address
        let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

        let db = Arc::new(Mutex::new(HashMap::new()));

        let connection_count = Arc::new(AtomicUsize::new(0));
        loop {
            let count = connection_count.clone();
            // The second item contains the IP and port of the new connection.
            let (socket, _) = listener.accept().await.unwrap();
            count.fetch_add(1, Ordering::SeqCst);
            tracing::debug!("new connection, total: {}", count.load(Ordering::SeqCst));
            let db = db.clone();
            tokio::task::Builder::new()
                .name(&format!("accept(socket={:?})", socket))
                .spawn(async move {
                    process(socket, db).await;
                    count.fetch_sub(1, Ordering::SeqCst);
                    tracing::debug!("end connection, total: {}", count.load(Ordering::SeqCst));
                });
        }
    }
}

mod process {
    use tokio::net::{TcpStream};
    use super::Db;

    pub(crate) async fn process(socket: TcpStream, db: Db) {
        use mini_redis::{Connection, Frame};
        use mini_redis::Command::{self, Get, Set};

        // Connection, provided by `mini-redis`, handles parsing frames from
        // the socket
        let mut connection = Connection::new(socket);

        // Use `read_frame` to receive a command from the connection.
        while let Some(frame) = connection.read_frame().await.unwrap() {
            tracing::debug!("mr-server received frame: {:?}", frame);
            let cmd = Command::from_frame(frame).unwrap();
            tracing::debug!("mr-server translated frame to cmd: {:?}", cmd);
            let response = match cmd {
                Set(cmd) => {
                    let mut db = db.lock().unwrap();
                    // The value is stored as `Vec<u8>`
                    db.insert(cmd.key().to_string(), cmd.value().clone());
                    Frame::Simple("OK".to_string())
                }
                Get(cmd) => {
                    let db = db.lock().unwrap();
                    if let Some(value) = db.get(cmd.key()) {
                        // `Frame::Bulk` expects data to be of type `Bytes`. This
                        // type will be covered later in the tutorial. For now,
                        // `&Vec<u8>` is converted to `Bytes` using `into()`.
                        Frame::Bulk(value.clone().into())
                    } else {
                        Frame::Null
                    }
                }
                cmd => panic!("unimplemented {:?}", cmd),
            };

            // Write the response to the client
            tracing::debug!("mr-server writing response: {:?}", response);
            connection.write_frame(&response).await.unwrap();
        }
    }
}
