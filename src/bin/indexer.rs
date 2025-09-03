use ircstore::{IrcEvent, S2Store};
use irc::client::prelude::*;
use futures::StreamExt;
use chrono::Utc;
use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, env = "IRC_NICK", default_value = "ircstore")]
    nick: String,
    
    #[arg(short, long, env = "IRC_SERVER", default_value = "irc.rotko.net")]
    server: String,
    
    #[arg(short, long, env = "IRC_PORT", default_value = "6697")]
    port: u16,
    
    #[arg(short = 't', long, env = "IRC_USE_TLS", default_value = "true")]
    use_tls: bool,
    
    #[arg(short, long, env = "IRC_CHANNELS", value_delimiter = ',')]
    channels: Option<Vec<String>>,
    
    #[arg(short = 'a', long, env = "S2_AUTH_TOKEN")]
    s2_auth_token: Option<String>,
}

struct IrcIndexer {
    store: S2Store,
    irc_client: irc::client::Client,
    server: String,
}

impl IrcIndexer {
    async fn new(args: Args) -> Result<Self> {
        if let Some(token) = args.s2_auth_token {
            std::env::set_var("S2_AUTH_TOKEN", token);
        }
        
        let store = S2Store::new().await?;
        
        let channels = args.channels.unwrap_or_else(|| vec!["#support".to_string(), "#dev".to_string()]);
        
        for channel in &channels {
            store.ensure_stream(channel).await?;
            println!("Created stream for {}", channel);
        }
        
        store.ensure_stream("_global").await?;
        
        let irc_config = Config {
            server: Some(args.server.clone()),
            port: Some(args.port),
            use_tls: Some(args.use_tls),
            channels,
            nickname: Some(args.nick),
            ..Default::default()
        };
        
        println!("Connecting to {}:{} (TLS: {})", args.server, args.port, args.use_tls);
        let irc_client = irc::client::Client::from_config(irc_config).await?;
        irc_client.identify()?;
        
        Ok(Self { 
            store, 
            irc_client,
            server: args.server,
        })
    }
    
    async fn run(&mut self) -> Result<()> {
        let mut stream = self.irc_client.stream()?;
        println!("Connected! Waiting for messages...");
        
        while let Some(msg) = stream.next().await.transpose()? {
            let event = self.process_message(&msg)?;
            let stream_name = event.channel.clone().unwrap_or_else(|| "_global".to_string());
            
            if let Err(e) = self.store.append(&stream_name, &event).await {
                eprintln!("Failed to append: {}", e);
            }
            
            if let Command::PRIVMSG(target, text) = &msg.command {
                println!("[{}] {}: {}", target, event.nick.as_ref().unwrap_or(&"*".to_string()), text);
            }
        }
        
        Ok(())
    }
    
    fn process_message(&self, msg: &Message) -> Result<IrcEvent> {
        let (channel, nick, message) = match &msg.command {
           Command::PRIVMSG(target, text) => {
               (Some(target.clone()), msg.source_nickname().map(|s| s.to_string()), text.clone())
           }
           Command::JOIN(chan, _, _) => {
               (Some(chan.clone()), msg.source_nickname().map(|s| s.to_string()),
                format!("*** {} joined", msg.source_nickname().unwrap_or("*")))
           }
           Command::PART(chan, reason) => {
               let part_msg = if let Some(r) = reason {
                   format!("*** {} left ({})", msg.source_nickname().unwrap_or("*"), r)
               } else {
                   format!("*** {} left", msg.source_nickname().unwrap_or("*"))
               };
               (Some(chan.clone()), msg.source_nickname().map(|s| s.to_string()), part_msg)
           }
           Command::QUIT(reason) => {
               let quit_msg = if let Some(r) = reason {
                   format!("*** {} quit ({})", msg.source_nickname().unwrap_or("*"), r)
               } else {
                   format!("*** {} quit", msg.source_nickname().unwrap_or("*"))
               };
               (None, msg.source_nickname().map(|s| s.to_string()), quit_msg)
           }
           Command::TOPIC(chan, topic) => {
               let topic_msg = if let Some(t) = topic {
                   format!("*** Topic set to: {}", t)
               } else {
                   "*** Topic cleared".to_string()
               };
               (Some(chan.clone()), msg.source_nickname().map(|s| s.to_string()), topic_msg)
           }
            }
            _ => (None, None, format!("{:?}", msg.command))
        };
        
        Ok(IrcEvent {
            timestamp: Utc::now().timestamp(),
            server: self.server.clone(),
            channel,
            nick,
            message,
            raw: format!("{}", msg),
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    
    let default_channels = vec!["#support".to_string(), "#dev".to_string()];
    let channels = args.channels.as_ref().unwrap_or(&default_channels);
    println!("Starting IRC indexer for channels: {:?}", channels);
    
    let mut indexer = IrcIndexer::new(args).await?;
    indexer.run().await?;
    
    Ok(())
}
