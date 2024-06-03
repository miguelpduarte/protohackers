use color_eyre::Result;
use deku::prelude::*;
use std::collections::BTreeMap;
use std::ops::Bound;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt().compact().init();

    color_eyre::install()?;

    let listener = TcpListener::bind("localhost:8008").await?;

    tracing::info!("Started server at {:?}", listener.local_addr());

    loop {
        let (socket, addr) = listener.accept().await?;

        tracing::debug!(%addr, "connection");
        let f = handle_connection(socket, addr);

        tokio::spawn(async move {
            if let Err(err) = f.await {
                tracing::error!(%addr, ?err, "error handling connection");
            }
        });
    }
}

#[derive(DekuRead, Debug)]
#[deku(id_type = "u8", endian = "big")] // endianness defaults to system, so just making sure.
enum Message {
    #[deku(id = 0x49)] // 'I'
    Insert { timestamp: i32, price: i32 },
    #[deku(id = 0x51)] // 'Q'
    Query { min_time: i32, max_time: i32 },
}

#[derive(DekuWrite, Debug)]
#[deku(endian = "big")]
struct AveragePrice(i32);

#[tracing::instrument(skip(stream))]
async fn handle_connection(
    mut stream: TcpStream,
    addr: core::net::SocketAddr,
) -> color_eyre::Result<()> {
    let mut buf = [0; 9];
    let mut state: BTreeMap<i32, i32> = BTreeMap::new();

    loop {
        tracing::debug!("waiting for request");
        let n = match stream.read_exact(&mut buf).await {
            // EOF
            Ok(0) => {
                tracing::debug!("Closing stream after EOF");
                return Ok(());
            }
            Ok(n) => n,
            Err(e) => {
                tracing::error!(err = %e, "failed to read from stream");
                color_eyre::eyre::bail!("Error reading from stream");
            }
        };

        tracing::debug!(n, "got bytes");
        tracing::debug!("raw data: {:?}", &buf);

        let (_, msg) = Message::from_bytes((&buf, 0)).inspect_err(|e| {
            tracing::error!(err=?e, "Error parsing message bytes");
        })?;

        match msg {
            Message::Insert { timestamp, price } => {
                state.insert(timestamp, price);
            }
            Message::Query { min_time, max_time } => {
                let response = AveragePrice(calculate_avg(min_time, max_time, &state));
                let response_data = response.to_bytes().inspect_err(|e| {
                    tracing::error!(err=?e, "Error converting response to bytes");
                })?;

                if let Err(e) = stream.write(&response_data).await {
                    tracing::error!(err = %e, "failed to write to stream");
                    color_eyre::eyre::bail!("Error writing to stream");
                }
            }
        }
    }
}

fn calculate_avg(min_time: i32, max_time: i32, state: &BTreeMap<i32, i32>) -> i32 {
    if min_time > max_time {
        return 0;
    }

    let mut sum = 0;
    let mut count = 0;
    for (&_key, &value) in state.range((Bound::Included(min_time), Bound::Included(max_time))) {
        sum += value as i64;
        count += 1;
    }

    sum.checked_div(count)
        .unwrap_or(0)
        .try_into()
        .expect("average doesn't fit in i32")
}
