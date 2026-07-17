use crate::types::{PlRequest, Event, Message, NodeId};
use tokio::sync::mpsc;
use std::collections::HashMap;
use tokio::net::TcpListener;
use std::collections::HashSet;
use sha2::{Sha256, Digest};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::io::{AsyncWriteExt, AsyncReadExt};

pub struct PerfectLink {
    // Consensus -> Network
    request_receiver: mpsc::Receiver<PlRequest>,
    // Network -> Consensus
    event_sender: mpsc::Sender<Event>, 
    internal_receiver: mpsc::Receiver<Vec<u8>>,

    peer_addresses: HashMap<NodeId, String>,
    active_sockets: HashMap<NodeId, OwnedWriteHalf>,

    seen_messages: HashSet<String>,
}

impl PerfectLink {
    pub fn new(
        request_receiver: mpsc::Receiver<PlRequest>,
        event_sender: mpsc::Sender<Event>,
        internal_receiver: mpsc::Receiver<Vec<u8>>,
        peer_addresses: HashMap<NodeId, String>,
    ) -> Self {
        PerfectLink {
            request_receiver,
            event_sender,
            internal_receiver,
            peer_addresses,
            active_sockets: HashMap::new(), 
            seen_messages: HashSet::new(),
        }
    }

    async fn send_to_peer(&mut self, dest: NodeId, raw_bytes: &[u8]) {
        if !self.active_sockets.contains_key(&dest) {
            let Some(addr) = self.peer_addresses.get(&dest) else { return; };
            let Ok(stream) = tokio::net::TcpStream::connect(addr).await else { return; };
            let (_, write_half) = stream.into_split();
            self.active_sockets.insert(dest, write_half);   
        }

        let Some(peer_socket) = self.active_sockets.get_mut(&dest) else { return; };
        
        let len = raw_bytes.len() as u32;
        if peer_socket.write_u32(len).await.is_err() || peer_socket.write_all(raw_bytes).await.is_err() {
            self.active_sockets.remove(&dest);
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(request) = self.request_receiver.recv() => {
                    match request {
                        PlRequest::Send { dest, msg } => {
                            let raw_bytes = serde_json::to_vec(&msg).unwrap();
                            self.send_to_peer(dest, &raw_bytes).await;
                        }
                        PlRequest::Broadcast { msg } => {
                            let raw_bytes = serde_json::to_vec(&msg).unwrap();
                            // clone the keys so we don't borrow self while calling self.send_to_peer
                            let peers: Vec<NodeId> = self.peer_addresses.keys().cloned().collect();
                            for node_id in peers {
                                self.send_to_peer(node_id, &raw_bytes).await;
                            }
                        }
                    }
                }

                Some(raw_bytes) = self.internal_receiver.recv() => {

                    let mut hasher = Sha256::new();
                    hasher.update(&raw_bytes);
                    let hash_result = hasher.finalize().iter().map(|b| format!("{:02x}", b)).collect::<String>();

                    if self.seen_messages.contains(&hash_result) {
                        continue;
                    }
                    self.seen_messages.insert(hash_result);

                    let msg: Message = match serde_json::from_slice(&raw_bytes) {
                        Ok(m) => m,
                        Err(_) => {
                            continue; 
                        }
                    };

                    let src = match &msg {
                        Message::Proposal(proposal) => proposal.sender,
                        Message::Vote(vote) => vote.sender,
                    };

                    self.event_sender.send(Event::PlDeliver { src, msg }).await.unwrap();
                    
                    let _len = raw_bytes.len() as u32;
                    let peers: Vec<NodeId> = self.peer_addresses.keys().cloned().collect();
                    for node_id in peers {
                        self.send_to_peer(node_id, &raw_bytes).await;
                    }
                }
            }
        }
    }   

    pub async fn start_listener(port: u16, internal_sender: mpsc::Sender<Vec<u8>>) {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();  

        tokio::spawn(async move {
            loop {
                let(mut socket, _addr) = listener.accept().await.unwrap();
                let sender_clone = internal_sender.clone();

                tokio::spawn(async move {
                    loop {

                        let len = match socket.read_u32().await {
                            Ok(l) => l as usize,
                            Err(_) => break, // Connection closed or error occurred
                        };

                        let mut buf = vec![0u8; len];
                        if let Err(_) = socket.read_exact(&mut buf).await {
                            break; // Connection closed or error occurred
                        }

                        if sender_clone.send(buf).await.is_err() {
                            break; // drop loop if main crashed
                        }
                    }
                });
            }
        });
    }
}


