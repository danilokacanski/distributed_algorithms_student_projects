use tracing::info;

use crate::types::{PlRequest, Message, NodeId, Height, Round, Step, Value, Proposal, Vote, VoteType, Behavior};
use std::collections::HashMap;
use std::time::Duration;
pub struct ConsensusState {
    pub id: NodeId, 
    pub height: Height,
    pub round: Round,
    pub step: Step,
    locked_value: Option<Value>,
    locked_round: Option<Round>,
    valid_value: Option<Value>,
    valid_round: Option<Round>,
    proposal: Option<Proposal>,
    votes: HashMap<Round, RoundVote>,
    total_nodes: u32,
    pub behavior: Behavior,
}

pub struct RoundVote {
    pub prevotes: HashMap<NodeId, Vote>,
    pub precommits: HashMap<NodeId, Vote>,
}

impl ConsensusState {
    pub fn new(id: NodeId, total_nodes: u32, behavior: Behavior) -> Self {
        ConsensusState {
            id,
            height: 1,
            round: 0,
            step: Step::Propose,
            locked_value: None,
            locked_round: None,
            valid_value: None,
            valid_round: None,
            proposal: None,
            votes: HashMap::new(),
            total_nodes,
            behavior,
        }
    }

    pub fn start_round(&mut self, round: Round) -> Option<PlRequest> {
        if self.behavior == Behavior::Silent { return None; }

        self.round = round;
        self.step = Step::Propose;
        self.proposal = None;
        self.votes.insert(self.round, RoundVote {
            prevotes: HashMap::new(),
            precommits: HashMap::new(),
        });

        info!("---------------------------------------------------------");
        info!("[Node {}] Starting Height: {}, Round: {}", self.id, self.height, self.round);
        info!("---------------------------------------------------------");

        if self.get_proposer() == self.id {
            let default_block = format!("Block Data for Height {}", self.height);
            let mut value_to_propose = self.valid_value.clone().unwrap_or(Value { data: default_block });
            
            if self.behavior == Behavior::SendInvalid {
                value_to_propose = Value { data: "MALICIOUS BLOCK".to_string() };
            }

            info!("[Node {}] [PROPOSER] I am the proposer for Height {}, Round {}. Proposing value: '{}'", self.id, self.height, self.round, value_to_propose.data);
        
            let proposal = Proposal {
                height: self.height,
                round: self.round,
                value: value_to_propose,
                valid_round: self.valid_round,
                sender: self.id,
            };

            return Some(PlRequest::Broadcast { msg: Message::Proposal(proposal) });
        }

        None
    }

    pub fn get_proposer(&self) -> NodeId {
        ((self.height + self.round as u64) % self.total_nodes as u64) as NodeId
    }

    pub fn handle_proposal(&mut self, proposal: Proposal) -> Option<PlRequest> {
        if self.behavior == Behavior::Silent { return None; }

        if self.step != Step::Propose || proposal.round != self.round || proposal.height != self.height {
            return None;
        }

        if proposal.sender != self.get_proposer() {
            return None;
        }

        let mut vote_value = Some(proposal.value.clone());
        
        if let Some(locked) = &self.locked_value {
            if locked != &proposal.value {
                vote_value = None;
            }
        }

        if let Some(vr) = proposal.valid_round {
            let quorum_size: usize = ((self.total_nodes * 2) / 3 + 1) as usize;
            
            let has_quorum = self.votes.get(&vr).map(|rv| {
                rv.prevotes.values()
                    .filter(|v| v.value == Some(proposal.value.clone()))
                    .count() >= quorum_size
            }).unwrap_or(false);

            if !has_quorum {
                vote_value = None;
            }
        }

        self.proposal = Some(proposal);
        self.step = Step::Prevote;

        let vote = Vote {
            vote_type: VoteType::Prevote,
            height: self.height,
            round: self.round,
            value: vote_value,
            sender: self.id,
        };

        Some(PlRequest::Broadcast { msg: Message::Vote(vote) })
    }

    pub fn handle_vote(&mut self, vote: Vote) -> Option<PlRequest> {
        if self.behavior == Behavior::Silent { return None; }

        if vote.height != self.height || vote.round != self.round {
            return None;
        }

        let quorum_size: usize = ((self.total_nodes * 2) / 3 + 1)  as usize;

        let round_votes = self.votes.entry(vote.round).or_insert(RoundVote {
            prevotes: HashMap::new(),
            precommits: HashMap::new(),
        });

        match vote.vote_type {
            VoteType::Prevote => {
                round_votes.prevotes.insert(vote.sender, vote.clone());
                
                let count = round_votes.prevotes.values()
                    .filter(|v| v.value == vote.value)
                    .count();

                if count >= quorum_size && self.step == Step::Prevote {
                    let val_str = vote.value.as_ref().map(|v| v.data.clone()).unwrap_or_else(|| "NIL".to_string());
                    info!("[Node {}] [PREVOTE QUORUM] Reached ({} votes) for value: '{}'. Moving to Precommit step.", self.id, count, val_str);
                    
                    if vote.value.is_some() {
                        self.locked_value = vote.value.clone();
                        self.locked_round = Some(vote.round);
                        self.valid_value = vote.value.clone();
                        self.valid_round = Some(vote.round);
                    }
                    
                    self.step = Step::Precommit;
                    let precommit_vote = Vote {
                        vote_type: VoteType::Precommit,
                        height: self.height,
                        round: self.round,
                        value: vote.value.clone(),
                        sender: self.id,
                    };

                    return Some(PlRequest::Broadcast { msg: Message::Vote(precommit_vote) });
                }
            }
            VoteType::Precommit => {
                round_votes.precommits.insert(vote.sender, vote.clone());

                let count = round_votes.precommits.values()
                    .filter(|v| v.value == vote.value)
                    .count();

                if count >= quorum_size && self.step == Step::Precommit {
                    if let Some(decided_value) = &vote.value {
                        info!("[Node {}] *** BLOCK COMMITTED ***", self.id);
                        info!("                Height: {}", self.height);
                        info!("                Value:  '{}'\n", decided_value.data);
                        self.step = Step::Commit;
                        return None;
                    } else {
                        info!("[Node {}] [PRECOMMIT NIL] Quorum reached for NIL. Network failed to agree. Moving to Round {}.", self.id, self.round + 1);
                        let next_round = self.round + 1;
                        return self.start_round(next_round);
                    }
               }
            } 
        }
        
        None 
    }

    pub fn timeout_duration(&self) -> Duration {
        let base_duration = match self.step {
            Step::Propose => 3000,
            Step::Prevote => 2000,
            Step::Precommit => 2000,
            Step::Commit => {
                let timeout = std::env::var("COMMIT_TIMEOUT")
                    .unwrap_or_else(|_| "1000".to_string())
                    .parse()
                    .unwrap_or(1000);
                return Duration::from_millis(timeout);
            }
        };
        
        let extra = (self.round as u64) * 500;
        Duration::from_millis(base_duration + extra)
    }

    pub fn handle_timeout(&mut self) -> Option<PlRequest> {
        if self.behavior == Behavior::Silent { return None; }

        match self.step {
            Step::Propose => {
                self.step = Step::Prevote;
                
                let vote = Vote {
                    vote_type: VoteType::Prevote,
                    height: self.height,
                    round: self.round,
                    value: None, // Nil vote
                    sender: self.id,
                };
                
                Some(PlRequest::Broadcast { msg: Message::Vote(vote) })
            }
            Step::Prevote => {
                self.step = Step::Precommit;
                
                let vote = Vote {
                    vote_type: VoteType::Precommit,
                    height: self.height,
                    round: self.round,
                    value: None, // Nil vote
                    sender: self.id,
                };
                
                Some(PlRequest::Broadcast { msg: Message::Vote(vote) })
            }
            Step::Precommit => {
                let next_round = self.round + 1;
                Some(self.start_round(next_round)?)
            }
            Step::Commit => {
                self.height += 1;
                // fresh height
                self.locked_round = None;
                self.locked_value = None;
                self.valid_round = None;
                self.valid_value = None;

                self.start_round(0)
            }
        }
    }
}
