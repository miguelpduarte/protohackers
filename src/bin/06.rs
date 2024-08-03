use color_eyre::{eyre::WrapErr, Result};
use deku::prelude::*;
use std::str::FromStr;
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

        tracing::info!(%addr, "connection");
        let f = handle_connection(socket, addr);

        tokio::spawn(async move {
            if let Err(err) = f.await {
                tracing::error!(%addr, ?err, "error handling connection");
            }
        });
    }
}

// TODO: unit tests to check this impl
#[derive(Debug, PartialEq, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    ctx = "endian: deku::ctx::Endian",
    ctx_default = "deku::ctx::Endian::Big"
)]
struct DekuableString {
    len: u8,
    #[deku(count = "len")]
    data: Vec<u8>,
}

impl ToString for DekuableString {
    fn to_string(&self) -> String {
        String::from_utf8(self.data.clone()).expect("valid utf8")
    }
}

impl FromStr for DekuableString {
    // TODO: Improve, this is just hacky to get it working. Could use infallible and panic as well
    type Err = String;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        let data = s.to_string().into_bytes();
        Ok(Self {
            len: u8::try_from(data.len())
                .map_err(|_| String::from("String too big for protocol"))?,
            data,
        })
    }
}

#[derive(Debug, DekuRead)]
#[deku(id_type = "u8", endian = "big")] // endianness defaults to system, so just making sure.
enum ClientMessage {
    #[deku(id = 0x20)]
    Plate {
        plate: DekuableString,
        timestamp: u32,
    },
    #[deku(id = 0x40)]
    WantHeartbeat {
        /// deciseconds
        interval: u32,
    },
    #[deku(id = 0x80)]
    IAmCamera {
        road: u16,
        mile: u16,
        /// miles per hour
        limit: u16,
    },
    #[deku(id = 0x81)]
    IAmDispatcher {
        numroads: u8,
        #[deku(count = "numroads")]
        roads: Vec<u16>,
    },
}

#[derive(DekuWrite, Debug)]
#[deku(id_type = "u8", endian = "big")] // endianness defaults to system, so just making sure.
enum ServerMessage {
    #[deku(id = 0x10)]
    //TODO: I think the string needs to have the attr implemented in a hacky way, might need a
    // newtype trick to get around trait impl shenanigans
    Error { msg: DekuableString },
    #[deku(id = 0x21)]
    Ticket {
        plate: DekuableString,
        road: u16,
        mile1: u16,
        timestamp1: u32,
        mile2: u16,
        timestamp2: u32,
        /// 100x miles per hour
        speed: u16,
    },
    #[deku(id = 0x41)]
    Heartbeat,
}

#[tracing::instrument(skip(stream))]
async fn handle_connection(
    mut stream: TcpStream,
    addr: core::net::SocketAddr,
) -> color_eyre::Result<()> {
    let mut buf = Vec::with_capacity(20);

    let bytes_read = match stream.read(&mut buf).await {
        Ok(0) => {
            tracing::debug!("Closing stream after EOF");
            return Ok(());
        }
        Ok(n) => n,
        Err(err) => {
            tracing::error!(%err, "failed to read from stream");
            color_eyre::eyre::bail!("Error reading from stream");
        }
    };

    // TODO: More than 1 message worth of bytes...
    let (_rest, msg) = match ClientMessage::from_bytes((&buf, 0)) {
        Ok(msg) => msg,
        Err(DekuError::Incomplete(n)) => {
            let n_bytes = n.byte_size();
            tracing::warn!(n_bytes, "missing bytes for parsing");
            unimplemented!("handle this case");
        }
        Err(DekuError::IdVariantNotFound) => {
            tracing::warn!("invalid message variant, reply with error and close connection");
            let response = ServerMessage::Error {
                msg: DekuableString::from_str("invalid message variant")
                    .expect("valid static string"),
            };
            let response_data = response.to_bytes().expect("valid static server error");
            let _ = stream.write(&response_data).await;
            return Ok(());
        }
        Err(_) => todo!(),
    };

    match msg {
        ClientMessage::Plate { .. } => panic!("unexpected message"),
        ClientMessage::WantHeartbeat { interval: _ } => todo!(),
        ClientMessage::IAmCamera { road, mile, limit } => {
            handle_camera(stream, road, mile, limit).await?
        }
        ClientMessage::IAmDispatcher { numroads: _, roads } => {
            handle_dispatcher(stream, roads).await?
        }
    }

    Ok(())
}

#[tracing::instrument(skip(stream))]
async fn handle_camera(
    stream: TcpStream,
    road: u16,
    mile: u16,
    limit: u16,
) -> color_eyre::Result<()> {
    todo!()
}

async fn handle_dispatcher(mut stream: TcpStream, roads: Vec<u16>) -> color_eyre::Result<()> {
    let msg = ServerMessage::Ticket {
        plate: DekuableString::from_str("UN1X").expect("valid static str"),
        road: 42,
        mile1: 10,
        timestamp1: 100,
        mile2: 110,
        timestamp2: 101,
        speed: 1230,
    };

    let msg_data = msg.to_bytes().inspect_err(|e| {
        tracing::error!(err=?e, "Error converting ticket message to bytes");
    })?;

    if let Err(e) = stream.write(&msg_data).await {
        tracing::error!(err = %e, "failed to write to stream");
        color_eyre::eyre::bail!("Error writing to stream");
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_dekustring_deserialize() {
        // Test cases in problem statement

        let data = vec![0x00];
        let (rest, parsed) = DekuableString::from_bytes((data.as_ref(), 0)).unwrap();
        assert_eq!(
            DekuableString {
                len: 0,
                data: vec![]
            },
            parsed
        );
        assert_eq!(rest.1, 0);
        assert_eq!(rest.0, vec![]);

        let data = vec![0x03, 0x66, 0x6f, 0x6f];
        let (rest, parsed) = DekuableString::from_bytes((data.as_ref(), 0)).unwrap();
        assert_eq!("foo", parsed.to_string());
        assert_eq!(rest.1, 0);
        assert_eq!(rest.0, vec![]);

        let data = vec![0x08, 0x45, 0x6C, 0x62, 0x65, 0x72, 0x65, 0x74, 0x68];
        let (rest, parsed) = DekuableString::from_bytes((data.as_ref(), 0)).unwrap();
        assert_eq!("Elbereth", parsed.to_string());
        assert_eq!(rest.1, 0);
        assert_eq!(rest.0, vec![]);

        // Additional with leftover data
        let data = vec![0x03, 0x66, 0x6f, 0x6f, 0x42];
        let (rest, parsed) = DekuableString::from_bytes((data.as_ref(), 0)).unwrap();
        assert_eq!("foo", parsed.to_string());
        assert_eq!(rest.1, 0);
        assert_eq!(rest.0, vec![0x42]);
    }

    #[test]
    fn test_dekustring_roundtrip() {
        let data = DekuableString::from_str("my simple test").expect("valid static string");

        assert_eq!(String::from("my simple test"), data.to_string());
    }
}
