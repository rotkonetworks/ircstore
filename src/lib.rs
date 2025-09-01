use serde::{Deserialize, Serialize};
use s2::{Client, ClientConfig};
use s2::types::*;
use anyhow::Result;
use futures::StreamExt;
use tokio::sync::broadcast;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IrcEvent {
    pub timestamp: i64,
    pub server: String,
    pub channel: Option<String>,
    pub nick: Option<String>,
    pub message: String,
    pub raw: String,
}

pub struct S2Store {
    client: Client,
    tx: Arc<broadcast::Sender<IrcEvent>>,
    last_seq: Arc<RwLock<HashMap<String, u64>>>,
}

impl S2Store {
    pub async fn new() -> Result<Self> {
        let token = std::env::var("S2_AUTH_TOKEN")?;
        let config = ClientConfig::new(token);
        let client = Client::new(config);
        let (tx, _) = broadcast::channel(1000);
        
        Ok(Self { 
            client,
            tx: Arc::new(tx),
            last_seq: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    pub fn subscribe(&self) -> broadcast::Receiver<IrcEvent> {
        self.tx.subscribe()
    }
    
    pub async fn ensure_stream(&self, name: &str) -> Result<()> {
        let server = std::env::var("IRC_SERVER").unwrap_or_else(|_| "irc-rotko-net".to_string());
        let basin_name = server.replace('.', "-").replace(':', "-");
        let basin: BasinName = basin_name.parse()?;
        
        let _ = self.client.create_basin(
            CreateBasinRequest::new(basin.clone())
        ).await;
        
        let basin_client = self.client.basin_client(basin);
        let _ = basin_client.create_stream(
            CreateStreamRequest::new(name)
        ).await;
        
        Ok(())
    }
    
    pub async fn append(&self, stream: &str, event: &IrcEvent) -> Result<()> {
        let server = std::env::var("IRC_SERVER").unwrap_or_else(|_| "irc-rotko-net".to_string());
        let basin_name = server.replace('.', "-").replace(':', "-");
        let basin: BasinName = basin_name.parse()?;
        let stream_client = self.client.basin_client(basin).stream_client(stream);
        
        let data = serde_json::to_vec(event)?;
        let record = AppendRecord::new(data)?;
        let batch = AppendRecordBatch::try_from_iter([record])
            .map_err(|(_, _)| anyhow::anyhow!("Failed to create batch"))?;
        let input = AppendInput::new(batch);
        
        stream_client.append(input).await?;
        
        // Broadcast the event
        let _ = self.tx.send(event.clone());
        
        Ok(())
    }
    
    pub async fn read(&self, stream: &str, offset: u64, limit: usize) -> Result<Vec<IrcEvent>> {
        let server = std::env::var("IRC_SERVER").unwrap_or_else(|_| "irc-rotko-net".to_string());
        let basin_name = server.replace('.', "-").replace(':', "-");
        let basin: BasinName = basin_name.parse()?;
        let stream_client = self.client.basin_client(basin).stream_client(stream);
        
        let req = ReadRequest::new(
            ReadStart::SeqNum(offset)
        ).with_limit(
            ReadLimit::new().with_count(limit as u64)
        );
        
        let output = stream_client.read(req).await?;
        
        let mut events = Vec::new();
        if let ReadOutput::Batch(batch) = output {
            for record in batch.records {
                if let Ok(event) = serde_json::from_slice::<IrcEvent>(&record.body) {
                    events.push(event);
                }
            }
        }
        
        Ok(events)
    }
    
    pub async fn list_streams(&self) -> Result<Vec<String>> {
        let server = std::env::var("IRC_SERVER").unwrap_or_else(|_| "irc-rotko-net".to_string());
        let basin_name = server.replace('.', "-").replace(':', "-");
        let basin: BasinName = basin_name.parse()?;
        let basin_client = self.client.basin_client(basin);
        
        let resp = basin_client.list_streams(
            ListStreamsRequest::new()
        ).await?;
        
        Ok(resp.streams.into_iter().map(|s| s.name).collect())
    }
    
    pub async fn get_stream_info(&self, stream: &str) -> Result<u64> {
        let server = std::env::var("IRC_SERVER").unwrap_or_else(|_| "irc-rotko-net".to_string());
        let basin_name = server.replace('.', "-").replace(':', "-");
        let basin: BasinName = basin_name.parse()?;
        let stream_client = self.client.basin_client(basin).stream_client(stream);
        
        let info = stream_client.check_tail().await?;
        Ok(info.seq_num)
    }
    
    pub async fn tail(&self, stream: &str) -> Result<()> {
        let server = std::env::var("IRC_SERVER").unwrap_or_else(|_| "irc-rotko-net".to_string());
        let basin_name = server.replace('.', "-").replace(':', "-");
        let basin: BasinName = basin_name.parse()?;
        let stream_client = self.client.basin_client(basin).stream_client(stream);
        
        // Get current position
        let start_seq = {
            let last_seqs = self.last_seq.read().await;
            last_seqs.get(stream).copied()
        }.unwrap_or_else(|| {
            // If no position stored, get current tail
            0 // Could use get_stream_info here to start from end
        });
        
        let req = ReadSessionRequest::new(
            ReadStart::SeqNum(start_seq)
        );
        
        let mut stream_reader = stream_client.read_session(req).await?;
        
        while let Some(result) = stream_reader.next().await {
            if let Ok(output) = result {
                if let ReadOutput::Batch(batch) = output {
                    let mut last_seq = 0u64;
                    for record in batch.records {
                        last_seq = record.seq_num;
                        
                        // Only broadcast if this is a new message
                        let should_broadcast = {
                            let last_seqs = self.last_seq.read().await;
                            last_seqs.get(stream).map_or(true, |&s| record.seq_num > s)
                        };
                        
                        if should_broadcast {
                            if let Ok(event) = serde_json::from_slice::<IrcEvent>(&record.body) {
                                let _ = self.tx.send(event);
                            }
                        }
                    }
                    
                    // Update last seen sequence
                    if last_seq > 0 {
                        let mut last_seqs = self.last_seq.write().await;
                        last_seqs.insert(stream.to_string(), last_seq);
                    }
                }
            }
        }
        
        Ok(())
    }
}
