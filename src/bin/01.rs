use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::Instrument;

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
enum RequestMethod {
    IsPrime,
}

#[derive(Deserialize, Debug)]
struct Request {
    #[allow(dead_code)]
    method: RequestMethod,
    number: f32,
}

#[derive(Serialize, Debug)]
struct Response {
    method: RequestMethod,
    prime: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt().compact().init();

    let listener = TcpListener::bind("localhost:8008").await?;

    loop {
        let (socket, addr) = listener.accept().await?;

        // println!("got connection from {}", addr);
        tracing::debug!(%addr, "connection");

        tokio::spawn(
            async move {
                let mut buf_stream = BufReader::new(socket);
                let mut data = String::new();

                loop {
                    // println!("A:{addr} waiting for another request");
                    tracing::debug!("waiting for request");
                    let n = match buf_stream.read_line(&mut data).await {
                        // EOF
                        Ok(0) => {
                            tracing::info!("Closing stream after EOF");
                            return
                        },
                        Ok(n) => n,
                        Err(e) => {
                            tracing::error!(err = %e, "failed to read from socket or invalid UTF-8");
                            // eprintln!(
                            //     "failed to read from socket or invalid UTF-8 '{addr}'; err = {:?}",
                            //     e
                            // );
                            return;
                        }
                    };

                    // println!("got {} bytes, raw request: {}", n, &data);
                    // println!("A:{addr} got {n} bytes");
                    tracing::debug!(n, "got bytes");

                    let req: Request = match serde_json::from_str(&data) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!(err = %e, raw_request = data, "Invalid request");
                            // eprintln!("Invalid request: '{:?}'; err = {:?}", &data, e);
                            // Write back invalid response and close connection
                            let r = buf_stream.write(b"banana\n").await;
                            tracing::info!(?r, "result of writing invalid response");
                            return;
                        }
                    };
                    // println!("req: {:?}", req);

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

                    // println!("responding: {:?}", res);
                    // println!("req: {:?}, resp: {:?}", &req, &res);
                    // println!("A:{addr} {} - {}", req.number, res.prime);
                    tracing::info!(number = req.number, is_prime = res.prime, "responding to request");

                    // Write the response back
                    let res_data = serde_json::to_string(&res).expect("always valid JSON") + "\n";

                    let byte_count = res_data.as_bytes().len();
                    let write_res = buf_stream.write(res_data.as_bytes()).await;

                    tracing::info!(byte_count, ?write_res, "wrote response");

                    if let Err(e) = write_res {
                        tracing::error!(err = %e, "failed to write to socket");
                        // eprintln!("failed to write to socket '{addr}'; err = {:?}", e.kind());
                        return;
                    }

                    data.clear();
                }
            }
            .instrument(tracing::info_span!("handling_conn", %addr)),
        );
    }
}
