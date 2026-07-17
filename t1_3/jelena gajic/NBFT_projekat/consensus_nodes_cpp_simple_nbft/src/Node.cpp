#include "Node.hpp"

#include "Network.hpp"

#include <algorithm>
#include <utility>

std::string faultModeToString(FaultMode mode) {
    switch (mode) {
    case FaultMode::HONEST:
        return "HONEST";
    case FaultMode::WRONG_VALUE:
        return "WRONG_VALUE";
    case FaultMode::LOW_SIGNATURES:
        return "LOW_SIGNATURES";
    case FaultMode::SILENT:
        return "SILENT";
    }
    return "UNKNOWN";
}

std::string nodePhaseToString(NodePhase phase) {
    switch (phase) {
    case NodePhase::IDLE:
        return "IDLE";
    case NodePhase::PRE_PREPARE1:
        return "PRE_PREPARE1";
    case NodePhase::IN_PREPARE1:
        return "IN_PREPARE1";
    case NodePhase::IN_PREPARE2:
        return "IN_PREPARE2";
    case NodePhase::OUT_PREPARE:
        return "OUT_PREPARE";
    case NodePhase::COMMIT:
        return "COMMIT";
    case NodePhase::PRE_PREPARE2:
        return "PRE_PREPARE2";
    case NodePhase::DECIDED:
        return "DECIDED";
    }
    return "UNKNOWN";
}

Node::Node(int id, Network &network) : id_(id), network_(network) {}

Node::~Node() {
    stop();
}

void Node::configureTopology(int groupId, bool primary, bool representative) {
    groupId_ = groupId;
    primary_ = primary;
    representative_ = representative;
}

void Node::setFaultMode(FaultMode mode) {
    faultMode_ = mode;
}

void Node::reset() {
    std::lock_guard<std::mutex> lock(inboxMutex_);
    std::queue<Message> empty;
    inbox_.swap(empty);
    phase_ = NodePhase::IDLE;
    proposedValue_.clear();
    decidedValue_.clear();
    localVotes_.clear();
    localCertificateSent_ = false;
    nodeDecisionSent_ = false;
    normalGroupWeights_.clear();
    abnormalNodesByGroup_.clear();
    commitSent_ = false;
    finalBroadcastSent_ = false;
    replySent_ = false;
}

void Node::start() {
    std::lock_guard<std::mutex> lock(inboxMutex_);
    if (running_) {
        return;
    }
    running_ = true;
    worker_ = std::thread(&Node::run, this);
}

void Node::stop() {
    {
        std::lock_guard<std::mutex> lock(inboxMutex_);
        running_ = false;
    }
    inboxReady_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
}

void Node::enqueue(Message message) {
    {
        std::lock_guard<std::mutex> lock(inboxMutex_);
        inbox_.push(std::move(message));
    }
    inboxReady_.notify_one();
}

int Node::id() const {
    return id_;
}

int Node::groupId() const {
    return groupId_;
}

bool Node::isPrimary() const {
    return primary_;
}

bool Node::isRepresentative() const {
    return representative_;
}

FaultMode Node::faultMode() const {
    return faultMode_;
}

NodePhase Node::phase() const {
    return phase_;
}

const std::string &Node::decidedValue() const {
    return decidedValue_;
}

void Node::run() {
    while (true) {
        Message message;
        {
            std::unique_lock<std::mutex> lock(inboxMutex_);
            inboxReady_.wait(lock, [this] { return !running_ || !inbox_.empty(); });
            if (!running_ && inbox_.empty()) {
                return;
            }
            message = std::move(inbox_.front());
            inbox_.pop();
        }
        handle(message);
    }
}

void Node::handle(const Message &message) {
    // Odluka je konacna; zakasnele poruke ne smeju vratiti cvor u staru fazu.
    if (phase_ == NodePhase::DECIDED) {
        return;
    }

    switch (message.type) {
    case MessageType::CLIENT_REQUEST:
        handleClientRequest(message);
        break;
    case MessageType::PRE_PREPARE1:
        handlePrePrepare1(message);
        break;
    case MessageType::IN_PREPARE1:
        handleInPrepare1(message);
        break;
    case MessageType::IN_PREPARE2:
        handleInPrepare2(message);
        break;
    case MessageType::OUT_PREPARE:
        handleOutPrepare(message);
        break;
    case MessageType::COMMIT:
        handleCommit(message);
        break;
    case MessageType::PRE_PREPARE2:
        handlePrePrepare2(message);
        break;
    case MessageType::REPLY:
        break;
    }
}

void Node::handleClientRequest(const Message &message) {
    if (!primary_ || faultMode_ == FaultMode::SILENT) {
        return;
    }

    view_ = message.view;
    sequenceNumber_ = message.sequenceNumber;
    proposedValue_ = message.value;
    phase_ = NodePhase::PRE_PREPARE1;
    network_.log("Primary Node " + std::to_string(id_) + " prihvata zahtev za " +
                 proposedValue_);

    Message proposal;
    proposal.type = MessageType::PRE_PREPARE1;
    proposal.from = id_;
    proposal.view = view_;
    proposal.sequenceNumber = sequenceNumber_;
    proposal.value = proposedValue_;
    network_.broadcastToReplicas(proposal);
}

void Node::handlePrePrepare1(const Message &message) {
    if (primary_ || faultMode_ == FaultMode::SILENT || message.from != network_.primaryId()) {
        return;
    }

    view_ = message.view;
    sequenceNumber_ = message.sequenceNumber;
    proposedValue_ = message.value;
    phase_ = NodePhase::IN_PREPARE1;

    Message vote;
    vote.type = MessageType::IN_PREPARE1;
    vote.from = id_;
    vote.to = network_.representativeForGroup(groupId_);
    vote.view = view_;
    vote.sequenceNumber = sequenceNumber_;
    vote.groupId = groupId_;
    vote.value = faultMode_ == FaultMode::WRONG_VALUE ? proposedValue_ + "_WRONG" : proposedValue_;
    vote.signatures.insert(id_);
    network_.send(vote);
}

void Node::handleInPrepare1(const Message &message) {
    if (!representative_ || faultMode_ == FaultMode::SILENT || message.groupId != groupId_ ||
        !network_.nodeBelongsToGroup(message.from, groupId_) || message.signatures.size() != 1 ||
        message.signatures.count(message.from) == 0) {
        return;
    }

    if (proposedValue_.empty()) {
        proposedValue_ = message.value;
        view_ = message.view;
        sequenceNumber_ = message.sequenceNumber;
    }

    auto &voters = localVotes_[message.value];
    voters.insert(message.from);
    if (localCertificateSent_ || voters.size() < static_cast<size_t>(network_.localQuorum())) {
        return;
    }

    localCertificateSent_ = true;
    phase_ = NodePhase::IN_PREPARE2;

    Message certificate;
    certificate.type = MessageType::IN_PREPARE2;
    certificate.from = id_;
    certificate.view = message.view;
    certificate.sequenceNumber = message.sequenceNumber;
    certificate.groupId = groupId_;
    certificate.value = message.value;
    certificate.signatures = voters;

    if (faultMode_ == FaultMode::LOW_SIGNATURES) {
        while (certificate.signatures.size() >= static_cast<size_t>(network_.localQuorum())) {
            certificate.signatures.erase(certificate.signatures.begin());
        }
    }

    network_.log("Grupa " + std::to_string(groupId_) + " formira lokalni sertifikat sa " +
                 std::to_string(certificate.signatures.size()) + " potpisa");
    network_.broadcastToGroup(certificate, groupId_, id_);

    if (certificate.signatures.size() >= static_cast<size_t>(network_.localQuorum())) {
        sendNormalOutPrepare(certificate);
    }
}

void Node::handleInPrepare2(const Message &message) {
    if (faultMode_ != FaultMode::HONEST || message.from != network_.representativeForGroup(groupId_) ||
        message.groupId != groupId_) {
        return;
    }

    phase_ = NodePhase::IN_PREPARE2;
    const bool sameValue = message.value == proposedValue_;
    const bool enoughSignatures =
        message.signatures.size() >= static_cast<size_t>(network_.localQuorum());
    const bool validSigners = validGroupSignatures(message.signatures, groupId_);

    if (!sameValue || !enoughSignatures || !validSigners) {
        sendNodeDecision("neispravan IN_PREPARE2 predstavnika");
    }
}

void Node::sendNormalOutPrepare(const Message &localCertificate) {
    phase_ = NodePhase::OUT_PREPARE;

    const int signatureCount = static_cast<int>(localCertificate.signatures.size());
    Message outPrepare;
    outPrepare.type = MessageType::OUT_PREPARE;
    outPrepare.from = id_;
    outPrepare.view = localCertificate.view;
    outPrepare.sequenceNumber = localCertificate.sequenceNumber;
    outPrepare.groupId = groupId_;
    outPrepare.value = localCertificate.value;
    outPrepare.signatures = localCertificate.signatures;
    outPrepare.voteWeight =
        signatureCount >= network_.groupSize() - network_.E() ? network_.groupSize()
                                                               : signatureCount;
    network_.broadcastToRepresentatives(outPrepare);
}

void Node::sendNodeDecision(const std::string &reason) {
    if (nodeDecisionSent_) {
        return;
    }
    nodeDecisionSent_ = true;
    phase_ = NodePhase::OUT_PREPARE;
    network_.log("Node " + std::to_string(id_) + " koristi node-decision: " + reason);

    Message outPrepare;
    outPrepare.type = MessageType::OUT_PREPARE;
    outPrepare.from = id_;
    outPrepare.view = view_;
    outPrepare.sequenceNumber = sequenceNumber_;
    outPrepare.groupId = groupId_;
    outPrepare.value = proposedValue_;
    outPrepare.signatures.insert(id_);
    outPrepare.voteWeight = 1;
    outPrepare.nodeDecision = true;
    network_.broadcastToOtherRepresentatives(outPrepare, groupId_);
}

void Node::handleOutPrepare(const Message &message) {
    if (!representative_ || faultMode_ != FaultMode::HONEST) {
        return;
    }

    phase_ = NodePhase::OUT_PREPARE;
    if (message.nodeDecision) {
        if (message.signatures.size() != 1 || message.signatures.count(message.from) == 0 ||
            !network_.nodeBelongsToGroup(message.from, message.groupId) ||
            message.from == network_.representativeForGroup(message.groupId)) {
            return;
        }
        if (normalGroupWeights_.count(message.groupId) == 0) {
            abnormalNodesByGroup_[message.groupId].insert(message.from);
        }
    } else {
        if (message.from != network_.representativeForGroup(message.groupId) ||
            message.signatures.size() < static_cast<size_t>(network_.localQuorum()) ||
            !validGroupSignatures(message.signatures, message.groupId)) {
            return;
        }

        const int F = static_cast<int>(message.signatures.size());
        normalGroupWeights_[message.groupId] =
            F >= network_.groupSize() - network_.E() ? network_.groupSize() : F;
        abnormalNodesByGroup_.erase(message.groupId);
    }

    trySendCommit();
}

void Node::trySendCommit() {
    if (commitSent_) {
        return;
    }

    int totalWeight = 0;
    for (const auto &[groupId, weight] : normalGroupWeights_) {
        totalWeight += weight;
    }
    for (const auto &[groupId, nodes] : abnormalNodesByGroup_) {
        if (normalGroupWeights_.count(groupId) == 0) {
            totalWeight += static_cast<int>(nodes.size());
        }
    }

    if (totalWeight < network_.globalThreshold()) {
        return;
    }

    commitSent_ = true;
    phase_ = NodePhase::COMMIT;
    network_.log("Predstavnik Node " + std::to_string(id_) + " dostize H=" +
                 std::to_string(totalWeight));

    Message commit;
    commit.type = MessageType::COMMIT;
    commit.from = id_;
    commit.to = network_.primaryId();
    commit.view = view_;
    commit.sequenceNumber = sequenceNumber_;
    commit.groupId = groupId_;
    commit.value = proposedValue_;
    commit.signatures.insert(id_);
    commit.voteWeight = totalWeight;
    network_.send(commit);
}

void Node::handleCommit(const Message &message) {
    if (!primary_ || faultMode_ != FaultMode::HONEST || finalBroadcastSent_ ||
        !network_.isRepresentative(message.from) || message.value != proposedValue_ ||
        message.voteWeight < network_.globalThreshold()) {
        return;
    }

    finalBroadcastSent_ = true;
    phase_ = NodePhase::PRE_PREPARE2;
    network_.log("Primary prihvata COMMIT dokaz H=" + std::to_string(message.voteWeight));

    Message finalMessage;
    finalMessage.type = MessageType::PRE_PREPARE2;
    finalMessage.from = id_;
    finalMessage.view = view_;
    finalMessage.sequenceNumber = sequenceNumber_;
    finalMessage.value = proposedValue_;
    finalMessage.voteWeight = message.voteWeight;
    network_.broadcastToReplicas(finalMessage);
    decideAndReply(proposedValue_);
}

void Node::handlePrePrepare2(const Message &message) {
    if (faultMode_ != FaultMode::HONEST || message.from != network_.primaryId() ||
        message.voteWeight < network_.globalThreshold() ||
        (!proposedValue_.empty() && message.value != proposedValue_)) {
        return;
    }

    phase_ = NodePhase::PRE_PREPARE2;
    if (proposedValue_.empty()) {
        proposedValue_ = message.value;
    }
    decideAndReply(message.value);
}

void Node::decideAndReply(const std::string &value) {
    if (replySent_) {
        return;
    }
    replySent_ = true;
    phase_ = NodePhase::DECIDED;
    decidedValue_ = value;
    network_.log("Node " + std::to_string(id_) + " ODLUCUJE " + value);

    Message reply;
    reply.type = MessageType::REPLY;
    reply.from = id_;
    reply.to = CLIENT_ID;
    reply.view = view_;
    reply.sequenceNumber = sequenceNumber_;
    reply.value = value;
    reply.signatures.insert(id_);
    network_.send(reply);
}

bool Node::validGroupSignatures(const std::set<int> &signatures, int groupId) const {
    return std::all_of(signatures.begin(), signatures.end(), [this, groupId](int signer) {
        return network_.nodeBelongsToGroup(signer, groupId);
    });
}
