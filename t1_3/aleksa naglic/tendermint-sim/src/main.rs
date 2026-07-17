pub mod consensus;
pub mod types;
pub mod perfect_link;

use crate::types::{Event, PlRequest, Message, Value, Behavior};
use crate::perfect_link::PerfectLink;
use tokio::sync::mpsc;
use std::collections::HashMap;
use tracing::info;
use std::env;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let node_id_str = env::var("NODE_ID").expect("NODE_ID environment variable not set");
    let node_id: u32 = node_id_str.parse().expect("NODE_ID must be a number");

    let total_nodes_str = env::var("TOTAL_NODES").unwrap_or_else(|_| "4".to_string());
    let total_nodes: u32 = total_nodes_str.parse().expect("TOTAL_NODES must be a number");

    // format like "1:node1:8000,2:node2:8001,3:node3:8002"
    let peers_str = env::var("PEERS").unwrap_or_else(|_| "".to_string());

    let mut peer_addresses = HashMap::new();
    for peer in peers_str.split(',') {
        if peer.is_empty() { continue; }

        let Some((id_str, addr)) = peer.split_once(':') else { continue; };
        
        let id: u32 = id_str.parse().expect("Peer ID must be a number");
        peer_addresses.insert(id, addr.to_string());
    }
    info!("Node ID: {}, peer_addresses: {:?}", node_id, peer_addresses);

    let(req_sender, req_receiver) = mpsc::channel(100);
    let(event_sender, mut event_receiver) = mpsc::channel(100);
    let(internal_sender, internal_receiver) = mpsc::channel(100);

    let port: u16 = (8000 + node_id) as u16;
    PerfectLink::start_listener(port, internal_sender.clone()).await;

    let mut perfect_link = PerfectLink::new(
        req_receiver,
        event_sender.clone(),
        internal_receiver,
        peer_addresses,
    );

    
    tokio::spawn(async move {
        perfect_link.run().await;
    });
    
    let behavior_str = env::var("BEHAVIOR").unwrap_or_else(|_| "standard".to_string());
    let behavior = match behavior_str.to_lowercase().as_str() {
        "silent" => Behavior::Silent,
        "double-vote" => Behavior::DoubleVote,
        "send-invalid" => Behavior::SendInvalid,
        _ => Behavior::Standard,
    };
    info!("Node ID: {}, Behavior: {:?}", node_id, behavior);

    let mut consensus = consensus::ConsensusState::new(node_id, total_nodes, behavior.clone());
    
    if let Some(req) = consensus.start_round(0) {
        req_sender.send(req).await.unwrap();
    }

    let sleep_timer = tokio::time::sleep(consensus.timeout_duration());
    tokio::pin!(sleep_timer);

    let telemetry_socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let monitor_addr = "monitor:9000";

    loop {
        tokio::select! {
            Some(event) = event_receiver.recv() => {
                tracing::debug!("Node {} received event: {:?}", node_id, event);
                
                let old_step = consensus.step.clone();

                let maybe_req = match event {
                    Event::PlDeliver { msg: Message::Proposal(p), .. } => consensus.handle_proposal(p),
                    Event::PlDeliver { msg: Message::Vote(v), .. } => consensus.handle_vote(v),
                    _ => None,
                };

                let mut reset_timer = old_step != consensus.step;

                if let Some(req) = maybe_req {
                    if behavior == Behavior::DoubleVote {
                        if let PlRequest::Broadcast { msg: Message::Vote(ref v) } = req {
                            req_sender.send(req.clone()).await.unwrap();
                            let mut evil_vote = v.clone();
                            evil_vote.value = Some(Value { data: "EVIL DOUBLE VOTE".to_string() });
                            req_sender.send(PlRequest::Broadcast { msg: Message::Vote(evil_vote) }).await.unwrap();
                            sleep_timer.as_mut().reset(tokio::time::Instant::now() + consensus.timeout_duration());
                            continue;
                        }
                    }

                    req_sender.send(req).await.unwrap();
                    reset_timer = true;
                }

                if reset_timer {
                    sleep_timer.as_mut().reset(tokio::time::Instant::now() + consensus.timeout_duration());
                }
            }

            _ = &mut sleep_timer => {
                tracing::debug!("Node {} timed out, transitioning step...", node_id);
                
                if let Some(req) = consensus.handle_timeout() {
                    if behavior == Behavior::DoubleVote {
                        if let PlRequest::Broadcast { msg: Message::Vote(ref v) } = req {
                            req_sender.send(req.clone()).await.unwrap();
                            let mut evil_vote = v.clone();
                            evil_vote.value = Some(Value { data: "EVIL DOUBLE VOTE".to_string() });
                            req_sender.send(PlRequest::Broadcast { msg: Message::Vote(evil_vote) }).await.unwrap();
                            sleep_timer.as_mut().reset(tokio::time::Instant::now() + consensus.timeout_duration());
                            continue;
                        }
                    }

                    req_sender.send(req).await.unwrap();
                }
                
                sleep_timer.as_mut().reset(tokio::time::Instant::now() + consensus.timeout_duration());
            }
        }
        
        let telemetry_json = format!(
            r#"{{"id": {}, "height": {}, "round": {}, "step": "{:?}"}}"#, 
            node_id, consensus.height, consensus.round, consensus.step
        );
        let _ = telemetry_socket.send_to(telemetry_json.as_bytes(), monitor_addr).await;
    }
}
