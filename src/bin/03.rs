use color_eyre::{eyre::WrapErr, Result};
use std::collections::HashSet;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, oneshot},
};

#[tokio::main]
async fn main() -> Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt().compact().init();

    color_eyre::install()?;

    let (chatevent_sender, mut _receiver) = broadcast::channel::<ChatEvent>(8);

    let listener = TcpListener::bind("localhost:8008").await?;

    tracing::info!("Started server at {:?}", listener.local_addr());

    // Debug logger task
    tokio::spawn(async move {
        loop {
            let msg = _receiver.recv().await.unwrap();
            tracing::info!(message = %msg, "chatevent");
        }
    });

    // User presence storage task
    // We can use the join and disconnect chat events to infer connected users
    let mut presence_chatevent_receiver = chatevent_sender.subscribe();
    // We will await in an mpsc channel with a oneshot to reply with a list of users.
    // No need for a "command" as the only operation is querying the connected users
    let (presence_query_sender, mut presence_query_receiver) =
        mpsc::channel::<oneshot::Sender<Presence>>(1);
    tokio::spawn(async move {
        // TODO: better error handling in this task, at least add some logging just in case.
        let mut connected_users: HashSet<String> = HashSet::new();
        loop {
            tokio::select! {
                chat_result = presence_chatevent_receiver.recv() => {
                    let chat_evt = chat_result.unwrap();
                    match chat_evt.etype {
                        // Ignore messages
                        ChatEventType::Message(_) => {}
                        ChatEventType::Connect => {
                            connected_users.insert(chat_evt.sender_username);
                        }
                        ChatEventType::Disconnect => {
                            connected_users.remove(&chat_evt.sender_username);
                        }
                    }
                }
                presence_replier = presence_query_receiver.recv() => {
                    let presence_replier = presence_replier
                        .expect("Error receiving presence query");

                    let user_list: Vec<_> = connected_users
                        .iter()
                        .cloned()
                        .collect();

                    presence_replier
                        .send(Presence::new(user_list))
                        .expect("Error replying to presence query");
                }
            }
        }
    });

    loop {
        let (socket, addr) = listener.accept().await?;

        tracing::info!(%addr, "connection");
        let f = handle_connection(
            socket,
            addr,
            chatevent_sender.clone(),
            presence_query_sender.clone(),
        );

        tokio::spawn(async move {
            if let Err(err) = f.await {
                tracing::error!(%addr, ?err, "error handling connection");
            }
        });
    }
}

#[derive(Debug)]
struct Presence {
    // TODO: Try using a &[&str] or Vec<&str> instead. Unnecessary optimization but good for
    // practicing
    users: Vec<String>,
}

impl Presence {
    fn new(users: Vec<String>) -> Self {
        Self { users }
    }
}

impl std::fmt::Display for Presence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let user_list = self.users.join(", ");
        writeln!(f, "* Chilling in the room there's: {}", user_list)
    }
}

#[derive(Clone, Debug)]
struct ChatEvent {
    sender_username: String,
    etype: ChatEventType,
}

#[derive(Clone, Debug)]
enum ChatEventType {
    Message(String),
    Connect,
    Disconnect,
}

impl std::fmt::Display for ChatEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.etype {
            ChatEventType::Message(message) => {
                // The message will already contain the newline so we don't need to trim that nor
                // add it.
                write!(f, "[{}] {}", self.sender_username, message)
            }
            ChatEventType::Connect => writeln!(f, "* {} is now here!", self.sender_username),
            ChatEventType::Disconnect => writeln!(f, "* {} peaced out!", self.sender_username),
        }
    }
}

#[tracing::instrument(skip(stream, bcast, presence_querier))]
async fn handle_connection(
    stream: TcpStream,
    addr: core::net::SocketAddr,
    bcast: broadcast::Sender<ChatEvent>,
    presence_querier: mpsc::Sender<oneshot::Sender<Presence>>,
) -> color_eyre::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut buf_read = tokio::io::BufReader::new(read);

    // TODO: Implement alternative solution with select! instead of subtask+oneshot

    if let Err(e) = write
        // Newline is necessary because the protocol specifies all messages to end with newline.
        .write_all(String::from("Welcome to cool-chat.rs! What's your username?\n").as_bytes())
        .await
    {
        tracing::error!(err = %e, "failed initial write to stream");
        color_eyre::eyre::bail!("Error in initial write to stream");
    }

    let mut username = String::new();
    if let Err(e) = buf_read.read_line(&mut username).await {
        tracing::error!(err = %e, "failed initial read from stream");
        color_eyre::eyre::bail!("Error in initial read from stream");
    }

    // Check the username validity, disconnect if invalid
    if !is_valid_username(username.trim()) {
        let _ = write.shutdown().await;
        tracing::warn!(
            username = username.trim(),
            "disconnecting user with invalid username"
        );
        return Ok(());
    }

    let username = username.trim().to_owned();

    // Requesting and sending presence notification before announcing joining so that the list
    // doesn't include ourselves
    let (presence_sender, presence_receiver) = oneshot::channel();
    presence_querier
        .send(presence_sender)
        .await
        .expect("Error sending presence query");
    let presence = presence_receiver
        .await
        .expect("Error getting presence response");
    if let Err(e) = write
        // Newline is necessary because the protocol specifies all messages to end with newline.
        .write_all(presence.to_string().as_bytes())
        .await
    {
        tracing::error!(err = %e, "failed presence writing to stream");
        color_eyre::eyre::bail!("Error in writing presence info to stream");
    }

    bcast
        .send(ChatEvent {
            sender_username: username.clone(),
            etype: ChatEventType::Connect,
        })
        .expect("Failed sending connect event");

    let mut event_rx = bcast.subscribe();

    let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();

    let our_username = username.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                raw_chat_event = event_rx
                   .recv()
                => {
                    let chat_event = raw_chat_event
                        .wrap_err("Error reading chat event")
                        .inspect_err(|e| tracing::error!(err=%e, "failure reading chat event"))
                        .unwrap();

                    // Ignore our own messages
                    if chat_event.sender_username == our_username {
                        continue;
                    }

                    write
                        .write_all(chat_event.to_string().as_bytes())
                        .await
                        .wrap_err("Error writing chat event to socket")
                        .inspect_err(|e|
                            tracing::error!(err=%e, "failure writing chat event to socket"))
                        .unwrap();
                }
                _ = &mut shutdown_receiver => {
                    break;
                }
            }
        }
    });

    let mut message = String::new();

    loop {
        tracing::debug!("waiting for chat message");
        let n = match buf_read.read_line(&mut message).await {
            // EOF
            Ok(0) => {
                tracing::info!(username, "client disconnected");
                let _ = shutdown_sender.send(());
                bcast
                    .send(ChatEvent {
                        sender_username: username,
                        etype: ChatEventType::Disconnect,
                    })
                    .unwrap();
                return Ok(());
            }
            Ok(n) => n,
            Err(e) => {
                tracing::error!(err = %e, "failed to read new chat messages from stream");
                color_eyre::eyre::bail!("Error reading from stream");
            }
        };

        tracing::debug!(n, "got bytes");
        tracing::debug!("raw data: {:?}", &message);

        bcast
            .send(ChatEvent {
                sender_username: username.clone(),
                etype: ChatEventType::Message(message.clone()),
            })
            .unwrap();

        message.clear();
    }
}

fn is_valid_username(username: &str) -> bool {
    // The first message from a client sets the user's name,
    // which must contain at least 1 character,
    // and must consist entirely of alphanumeric characters (uppercase, lowercase, and digits).
    username.len() > 0 && username.chars().all(|c| char::is_ascii_alphanumeric(&c))
}
