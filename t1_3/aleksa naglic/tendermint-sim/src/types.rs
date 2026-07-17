use serde::{Deserialize, Serialize};

pub type NodeId = u32;
pub type Height = u64;
pub type Round = u32;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Value { // Newtype pattern, for better type safety
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proposal {
    pub height: Height,
    pub round: Round,
    pub value: Value,
    pub valid_round: Option<Round>,
    pub sender: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vote {
    pub vote_type: VoteType,
    pub height: Height,
    pub round: Round,
    pub value: Option<Value>, // None represents a 'nil' vote
    pub sender: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum VoteType {
    Prevote,
    Precommit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Proposal(Proposal),
    Vote(Vote),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Step {
    Propose,
    Prevote,
    Precommit,
    Commit,
}

#[derive(Debug, Clone)]
pub enum PlRequest {
    // <pl, Send | dest, msg>
    Send {
        dest: NodeId,
        msg: Message,
    },
    // <beb, Broadcast | msg>
    Broadcast {
        msg: Message,
    },
}

#[derive(Debug, Clone)]
pub enum Event {
    PlDeliver {
        src: NodeId,
        msg: Message,
    },
    ProposeValue(Value),
    Timeout {
        round: Round,
        step: Step,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Behavior {
    Standard,
    Silent,
    DoubleVote,
    SendInvalid,
}

