use anyhow::{Context, Result};
use clap::Parser;
use rhio_client::Client;
use tokio::sync::mpsc;

/// Simple client to send p2panda operations to a NATS server.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Endpoint of NATS server.
    #[arg(short, long, default_value = "localhost:4222")]
    endpoint: String,

    /// Publish messages to this NATS subject.
    #[arg(short, long)]
    subject: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut client = Client::new_ephemeral(&args.endpoint)
        .await
        .context("connecting to NATS server")?;
    println!("connected to NATS server @ {}", args.endpoint);

    println!(
        "> publish messages with subject '{}' and hit enter:",
        args.subject
    );
    let (line_tx, mut line_rx) = tokio::sync::mpsc::channel(1);
    std::thread::spawn(move || input_loop(line_tx));

    loop {
        tokio::select! {
            Some(payload) = line_rx.recv() => {
                client
                    .publish(args.subject.clone(), payload.as_bytes())
                    .await?;
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }

    Ok(())
}

fn input_loop(line_tx: mpsc::Sender<String>) -> Result<()> {
    let mut buffer = String::new();
    let stdin = std::io::stdin();
    loop {
        stdin.read_line(&mut buffer)?;
        line_tx.blocking_send(buffer.clone())?;
        buffer.clear();
    }
}
