use serde::{Deserialize, Serialize};
use s2::{Client, ClientConfig};
use s2::types::*;
use anyhow::Result;
use futures::StreamExt;
use tokio::sync::broadcast;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};

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
    basin: BasinName,
    tx: Arc<broadcast::Sender<IrcEvent>>,
    last_seq: Arc<RwLock<HashMap<String, u64>>>,
    tail_tasks: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
}

impl S2Store {
    pub async fn new() -> Result<Self> {
        let token = std::env::var("S2_AUTH_TOKEN")?;
        let server = std::env::var("IRC_SERVER").unwrap_or_else(|_| "irc-rotko-net".to_string());
        let basin_name = server.replace('.', "-").replace(':', "-");
        let basin: BasinName = basin_name.parse()?;
        
        let config = ClientConfig::new(token);
        let client = Client::new(config);
        let (tx, _) = broadcast::channel(1000);
        
        Ok(Self { 
            client,
            basin,
            tx: Arc::new(tx),
            last_seq: Arc::new(RwLock::new(HashMap::new())),
            tail_tasks: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    pub fn subscribe(&self) -> broadcast::Receiver<IrcEvent> {
        self.tx.subscribe()
    }
    
    pub fn client_count(&self) -> usize {
        self.tx.receiver_count()
    }
    
    pub async fn stop_tails_if_idle(&self) {
        if self.client_count() == 0 {
            let mut tasks = self.tail_tasks.write().await;
            for (_, handle) in tasks.drain() {
                handle.abort();
            }
        }
    }
    
    pub async fn ensure_stream(&self, name: &str) -> Result<()> {
        let _ = self.client.create_basin(CreateBasinRequest::new(self.basin.clone())).await;
        let _ = self.client.basin_client(self.basin.clone()).create_stream(CreateStreamRequest::new(name)).await;
        Ok(())
    }
    
    pub async fn append(&self, stream: &str, event: &IrcEvent) -> Result<()> {
        let stream_client = self.client.basin_client(self.basin.clone()).stream_client(stream);
        let data = serde_json::to_vec(event)?;
        let record = AppendRecord::new(data)?;
        let batch = AppendRecordBatch::try_from_iter([record])
            .map_err(|(_, _)| anyhow::anyhow!("batch creation failed"))?;
        
        stream_client.append(AppendInput::new(batch)).await?;
        let _ = self.tx.send(event.clone());
        Ok(())
    }
    
    pub async fn read(&self, stream: &str, offset: u64, limit: usize) -> Result<Vec<IrcEvent>> {
        let stream_client = self.client.basin_client(self.basin.clone()).stream_client(stream);
        let req = ReadRequest::new(ReadStart::SeqNum(offset))
            .with_limit(ReadLimit::new().with_count(limit as u64));
        
        let output = stream_client.read(req).await?;
        let ReadOutput::Batch(batch) = output else { return Ok(vec![]) };
        
        Ok(batch.records.iter()
            .filter_map(|r| serde_json::from_slice::<IrcEvent>(&r.body).ok())
            .collect())
    }
    
    pub async fn list_streams(&self) -> Result<Vec<String>> {
        let resp = self.client.basin_client(self.basin.clone())
            .list_streams(ListStreamsRequest::new()).await?;
        Ok(resp.streams.into_iter().map(|s| s.name).collect())
    }
    
    pub async fn ensure_tail(&self, stream: &str) -> bool {
        let mut tasks = self.tail_tasks.write().await;
        tasks.retain(|_, h| !h.is_finished());
        
        if tasks.contains_key(stream) {
            return false;
        }
        
        let stream_name = stream.to_string();
        let store_clone = Self {
            client: self.client.clone(),
            basin: self.basin.clone(),
            tx: self.tx.clone(),
            last_seq: self.last_seq.clone(),
            tail_tasks: self.tail_tasks.clone(),
        };
        
        let handle = tokio::spawn(async move {
            loop {
                if let Err(e) = store_clone.tail_impl(&stream_name).await {
                    eprintln!("tail {}: {}", stream_name, e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });
        
        tasks.insert(stream.to_string(), handle);
        true
    }
    
    async fn tail_impl(&self, stream: &str) -> Result<()> {
        let stream_client = self.client.basin_client(self.basin.clone()).stream_client(stream);
        
        let start_seq = self.last_seq.read().await.get(stream).copied().unwrap_or(0);
        let mut stream_reader = stream_client.read_session(ReadSessionRequest::new(ReadStart::SeqNum(start_seq))).await?;
        
        loop {
            let result = match timeout(Duration::from_secs(5), stream_reader.next()).await {
                Ok(Some(r)) => r,
                Ok(None) => return Ok(()),
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            
            let batch = match result? {
                ReadOutput::Batch(b) if !b.records.is_empty() => b,
                _ => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            
            let mut last_seq = 0u64;
            let last_known = self.last_seq.read().await.get(stream).copied().unwrap_or(0);
            
            for record in batch.records {
                if record.seq_num <= last_known {
                    continue;
                }
                
                last_seq = record.seq_num;
                if let Ok(event) = serde_json::from_slice::<IrcEvent>(&record.body) {
                    let _ = self.tx.send(event);
                }
            }
            
            if last_seq > 0 {
                self.last_seq.write().await.insert(stream.to_string(), last_seq);
            }
        }
    }
}
