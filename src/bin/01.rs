use color_eyre::Result;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
enum RequestMethod {
    IsPrime,
}

#[derive(Deserialize, Debug)]
struct Request {
    #[allow(dead_code)]
    method: RequestMethod,
    number: f64,
}

#[derive(Serialize, Debug)]
struct Response {
    method: RequestMethod,
    prime: bool,
}

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

#[tracing::instrument(skip(stream))]
async fn handle_connection(
    stream: TcpStream,
    addr: core::net::SocketAddr,
) -> color_eyre::Result<()> {
    let mut buf_stream = BufReader::new(stream);
    let mut data = String::new();

    loop {
        tracing::debug!("waiting for request");
        let n = match buf_stream.read_line(&mut data).await {
            // EOF
            Ok(0) => {
                tracing::debug!("Closing stream after EOF");
                return Ok(());
            }
            Ok(n) => n,
            Err(e) => {
                tracing::error!(err = %e, "failed to read from stream or invalid UTF-8");
                color_eyre::eyre::bail!("Error reading from stream");
            }
        };

        tracing::debug!(n, "got bytes");

        let req: Request = match serde_json::from_str(&data) {
            Ok(r) => r,
            Err(e) => {
                tracing::info!(err = %e, raw_request = data, "Invalid request");
                // Write back invalid response and close connection
                let r = buf_stream.write(b"banana\n").await;
                tracing::debug!(?r, "result of writing invalid response");
                return Ok(());
            }
        };

        let is_prime = match req.number.fract() == 0.0 {
            // integer, check primality
            true => primal::is_prime(req.number.trunc() as u64),
            // floating point, never prime
            false => false,
        };

        let res = Response {
            method: RequestMethod::IsPrime,
            prime: is_prime,
        };

        tracing::info!(
            number = req.number,
            is_prime = res.prime,
            "responding to request"
        );

        // Write the response back
        let res_data = serde_json::to_string(&res).expect("always valid JSON") + "\n";

        let byte_count = res_data.as_bytes().len();
        let write_res = buf_stream.write(res_data.as_bytes()).await;

        tracing::info!(byte_count, ?write_res, "wrote response");
        if byte_count != write_res.as_ref().map_or(0, |x| *x) {
            tracing::error!("different bytes msg and written");
        }

        if let Err(e) = write_res {
            tracing::error!(err = %e, "failed to write to stream");
            color_eyre::eyre::bail!("Error writing to stream");
        }

        data.clear();
    }
}
