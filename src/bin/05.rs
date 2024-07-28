use color_eyre::{eyre::WrapErr, Result};
use regex::Regex;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
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

        tracing::info!(%addr, "connection");
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
    let (client_read, mut client_write) = stream.into_split();
    let mut client_buf_read = tokio::io::BufReader::new(client_read);

    tracing::debug!("connecting to upstream");
    let (upstream_read, mut upstream_write) = TcpStream::connect("chat.protohackers.com:16963")
        .await?
        .into_split();
    let mut upstream_buf_read = tokio::io::BufReader::new(upstream_read);
    tracing::debug!("upstream good to go");

    let mut client_buf = vec![];
    let mut server_buf = vec![];

    tracing::debug!("looping waiting for input");

    // Reading and forwarding to other half
    loop {
        tokio::select! {
            client_read_result = client_buf_read.read_until(b'\n', &mut client_buf) => {
                let client_read_bytes = match client_read_result {
                    Err(e) => {
                        tracing::error!(err = %e, "failed to read new chat messages from client stream");
                        color_eyre::eyre::bail!("Error reading from client stream");
                    }
                    Ok(read_bytes) => read_bytes,
                };
                // Testing for connection termination under 2 conditions:
                // - message without newline
                // - EOF detected via 0 bytes read
                // If so, just shut it down.
                if client_read_bytes == 0 || *client_buf.last().unwrap() != b'\n' {
                        upstream_write.shutdown().await?;
                        return Ok(());
                }

                // TODO: Probably remove the call to trim. Added because the old method with
                // the Lines iterator consumed the newline and thus the parsing logic doesn't need
                // to be touched.
                let client_msg = std::str::from_utf8(&client_buf).expect("valid utf8").trim_end_matches('\n');


                tracing::debug!(client_msg, "Read client msg");
                let client_msg = rewrite_addresses(client_msg);
                tracing::debug!(client_msg, "rewrote client msg");

                upstream_write
                    .write_all((client_msg + "\n").as_bytes())
                    .await
                    .wrap_err("Error writing message to server")
                    .inspect_err(|e| tracing::error!(err=%e, "failure writing message to server"))
                    .unwrap();

                client_buf.clear();
            }
            server_read_result = upstream_buf_read.read_until(b'\n', &mut server_buf) => {
                let server_read_bytes = match server_read_result {
                    Err(e) => {
                        tracing::error!(err = %e, "failed to read new chat messages from server stream");
                        color_eyre::eyre::bail!("Error reading from server stream");
                    }
                    Ok(read_bytes) => read_bytes,
                };
                // Testing for connection termination under 2 conditions:
                // - message without newline
                // - EOF detected via 0 bytes read
                // If so, just shut it down.
                if server_read_bytes == 0 || *server_buf.last().unwrap() != b'\n' {
                        client_write.shutdown().await?;
                        return Ok(());
                }
                // TODO: Probably remove the call to trim. Added because the old method with
                // the Lines iterator consumed the newline and thus the parsing logic doesn't need
                // to be touched.
                let server_msg = std::str::from_utf8(&server_buf).expect("valid utf8").trim_end_matches('\n');

                tracing::debug!(server_msg, "Read server msg");
                let server_msg = rewrite_addresses(server_msg);
                tracing::debug!(server_msg, "server msg post rewrite");

                client_write
                    .write_all((server_msg + "\n").as_bytes())
                    .await
                    .wrap_err("Error writing message to client")
                    .inspect_err(|e| tracing::error!(err=%e, "failure writing message to client"))
                    .unwrap();

                server_buf.clear();
            }
        }
    }
}

#[allow(dead_code)]
const TONYS_ADDRESS: &str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";
const TONYS_ADDRESS_REPLACEMENT: &str = "${pre}7YWHMfk9JZe0LM0g1ZauHuiSxhI$post";

fn rewrite_addresses(message: &str) -> String {
    // TODO: once_cell to only init regex once

    // TODO: match starting space OR ending space, not potentially both -> in the end it's the same
    // outcome I think

    // 25 to 34 because the leading 7 is already consumed
    // The alternative groups can likely be handled better, but this is an easy way to avoid
    // grabbing part of an address as valid (due to using ^$).
    // Capturing groups to ensure that spaces are not removed accidentally
    let boguscoin_addr_regex = Regex::new(
        // r"(?:(?<pre>(?:^)|(?: ))7[[:alnum:]]{25,34})|(?:7[[:alnum:]]{25,34}(?<post>(?: )|(?:$)))",
        "(?<pre>^| )?7[[:alnum:]]{25,34}(?<post>$| )",
    )
    .expect("static valid regex");
    boguscoin_addr_regex
        .replace_all(message, TONYS_ADDRESS_REPLACEMENT)
        .to_string()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_rewrite_addresses() {
        // No address = no rewrite
        let msg = String::from("Hi alice, hope all is well");
        let expected = String::from("Hi alice, hope all is well");
        assert_eq!(rewrite_addresses(&msg), expected);

        let msg = String::from("Hi alice, please send payment to 7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX");
        let expected = String::from("Hi alice, please send payment to 7YWHMfk9JZe0LM0g1ZauHuiSxhI");
        assert_eq!(rewrite_addresses(&msg), expected);

        let msg = String::from("7F1u3wSD5RbOHQmupo9nx4TnhQ address at the start");
        let expected = String::from("7YWHMfk9JZe0LM0g1ZauHuiSxhI address at the start");
        assert_eq!(rewrite_addresses(&msg), expected);

        let msg = String::from("7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX ");
        let expected = String::from("7YWHMfk9JZe0LM0g1ZauHuiSxhI ");
        assert_eq!(rewrite_addresses(&msg), expected);

        let msg = String::from(" 7LOrwbDlS8NujgjddyogWgIM93MV5N2VR");
        let expected = String::from(" 7YWHMfk9JZe0LM0g1ZauHuiSxhI");
        assert_eq!(rewrite_addresses(&msg), expected);

        let msg = String::from(" 7adNeSwJkMakpEcln9HEtthSRtxdmEHOT8T ");
        let expected = String::from(" 7YWHMfk9JZe0LM0g1ZauHuiSxhI ");
        assert_eq!(rewrite_addresses(&msg), expected);

        // Too long shouldn't be replaced (previous bug since we did partial matching)
        let msg = String::from("This is too long: 7L2FLjJJvFQhEv29VJygHY99xzLkfvgloaRx");
        let expected = String::from("This is too long: 7L2FLjJJvFQhEv29VJygHY99xzLkfvgloaRx");
        assert_eq!(rewrite_addresses(&msg), expected);

        // Another bug with multiple addresses since the old matching broke with spaces sometimes
        let msg = String::from("Please pay the ticket price of 15 Boguscoins to one of these addresses: 7Opb0suCE0CMLIyczQBVE2hsFxVQM8 7MnKIu4CIQqdqDdf9uVlRI2RUk 7YWHMfk9JZe0LM0g1ZauHuiSxhI");
        let expected = String::from("Please pay the ticket price of 15 Boguscoins to one of these addresses: 7YWHMfk9JZe0LM0g1ZauHuiSxhI 7YWHMfk9JZe0LM0g1ZauHuiSxhI 7YWHMfk9JZe0LM0g1ZauHuiSxhI");
        assert_eq!(rewrite_addresses(&msg), expected);
    }
}
