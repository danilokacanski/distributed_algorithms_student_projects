#include "Network.hpp"

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <thread>

Network::Network(int numberOfNodes, int groupSize)
    : numberOfNodes_(numberOfNodes), groupSize_(groupSize), E_((groupSize - 1) / 3),
      R_((numberOfNodes - 1) / groupSize), w_((R_ - 1) / 3), localQuorum_(2 * E_ + 1),
      globalThreshold_((R_ - w_) * groupSize), replyThreshold_((numberOfNodes - 1) / 2 + 1) {
    if (groupSize_ < 4 || (groupSize_ - 1) % 3 != 0) {
        throw std::runtime_error("Velicina grupe mora biti m=3E+1, npr. 4, 7 ili 10.");
    }
    if ((numberOfNodes_ - 1) % groupSize_ != 0) {
        throw std::runtime_error("Broj cvorova mora biti n=R*m+1.");
    }
    if (R_ < 4) {
        throw std::runtime_error("Rad zahteva najmanje R=4 grupe.");
    }
}

Network::~Network() {
    stop();
}

void Network::initialize() {
    buildTopology();
    nodes_.clear();
    for (int id = 0; id < numberOfNodes_; ++id) {
        nodes_.push_back(std::make_unique<Node>(id, *this));
    }

    for (int id = 0; id < numberOfNodes_; ++id) {
        const auto groupIt = nodeToGroup_.find(id);
        const int groupId = groupIt == nodeToGroup_.end() ? -1 : groupIt->second;
        const bool representative =
            groupId >= 0 && groups_[groupId].representativeId == id;
        nodes_[id]->configureTopology(groupId, id == primaryId_, representative);
        nodes_[id]->reset();
    }
}

void Network::configureScenario(const std::string &scenario, int byzantineCount) {
    for (auto &node : nodes_) {
        node->setFaultMode(FaultMode::HONEST);
    }

    if (scenario == "normal") {
        if (byzantineCount != 0) {
            throw std::runtime_error("Scenario normal zahteva 0 Byzantine cvorova.");
        }
        return;
    }
    if (byzantineCount < 1) {
        throw std::runtime_error("Izabrani scenario zahteva najmanje 1 Byzantine cvor.");
    }

    std::vector<int> ordinary = ordinaryNodeCandidates();
    if (scenario == "byzantine_wrong_value") {
        if (byzantineCount > static_cast<int>(ordinary.size())) {
            throw std::runtime_error("Nema dovoljno obicnih cvorova za izabrani scenario.");
        }
        for (int i = 0; i < byzantineCount; ++i) {
            nodes_[ordinary[i]]->setFaultMode(FaultMode::WRONG_VALUE);
        }
        return;
    }

    if (scenario == "faulty_rep_low") {
        nodes_[groups_.front().representativeId]->setFaultMode(FaultMode::LOW_SIGNATURES);
        if (byzantineCount - 1 > static_cast<int>(ordinary.size())) {
            throw std::runtime_error("Nema dovoljno cvorova za izabrani scenario.");
        }
        for (int i = 0; i < byzantineCount - 1; ++i) {
            nodes_[ordinary[i]]->setFaultMode(FaultMode::WRONG_VALUE);
        }
        return;
    }

    if (scenario == "primary_silent") {
        nodes_[primaryId_]->setFaultMode(FaultMode::SILENT);
        if (byzantineCount - 1 > static_cast<int>(ordinary.size())) {
            throw std::runtime_error("Nema dovoljno cvorova za izabrani scenario.");
        }
        for (int i = 0; i < byzantineCount - 1; ++i) {
            nodes_[ordinary[i]]->setFaultMode(FaultMode::WRONG_VALUE);
        }
        return;
    }

    throw std::runtime_error("Nepoznat scenario: " + scenario);
}

void Network::start() {
    for (auto &node : nodes_) {
        node->start();
    }
}

void Network::stop() {
    for (auto &node : nodes_) {
        node->stop();
    }
}

bool Network::runConsensus(const std::string &value, int timeoutMs) {
    {
        std::lock_guard<std::mutex> lock(clientMutex_);
        clientReplies_.clear();
        clientDecided_ = false;
        clientValue_.clear();
    }

    log("CLIENT salje zahtev primary cvoru Node " + std::to_string(primaryId_));
    Message request;
    request.type = MessageType::CLIENT_REQUEST;
    request.from = CLIENT_ID;
    request.to = primaryId_;
    request.view = view_;
    request.sequenceNumber = sequenceNumber_;
    request.value = value;
    send(request);

    std::unique_lock<std::mutex> lock(clientMutex_);
    const bool decided = clientReady_.wait_for(
        lock, std::chrono::milliseconds(timeoutMs), [this] { return clientDecided_; });
    lock.unlock();

    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    return decided;
}

void Network::send(Message message) {
    message.id = nextMessageId_.fetch_add(1);
    const std::string destination =
        message.to == CLIENT_ID ? "CLIENT" : "Node " + std::to_string(message.to);
    log("SEND " + messageTypeToString(message.type) + " | Node " +
        std::to_string(message.from) + " -> " + destination);

    if (message.to == CLIENT_ID) {
        receiveClientReply(message);
        return;
    }
    if (message.to < 0 || message.to >= numberOfNodes_) {
        log("Poruka je odbacena: primalac ne postoji.");
        return;
    }
    nodes_[message.to]->enqueue(std::move(message));
}

void Network::broadcastToReplicas(Message message) {
    for (int nodeId = 0; nodeId < numberOfNodes_; ++nodeId) {
        if (nodeId != primaryId_) {
            message.to = nodeId;
            send(message);
        }
    }
}

void Network::broadcastToGroup(Message message, int groupId, int excludedNode) {
    for (int nodeId : groups_.at(groupId).members) {
        if (nodeId != excludedNode) {
            message.to = nodeId;
            send(message);
        }
    }
}

void Network::broadcastToRepresentatives(Message message) {
    for (const Group &group : groups_) {
        message.to = group.representativeId;
        send(message);
    }
}

void Network::broadcastToOtherRepresentatives(Message message, int excludedGroup) {
    for (const Group &group : groups_) {
        if (group.id != excludedGroup) {
            message.to = group.representativeId;
            send(message);
        }
    }
}

int Network::primaryId() const {
    return primaryId_;
}

int Network::groupSize() const {
    return groupSize_;
}

int Network::E() const {
    return E_;
}

int Network::localQuorum() const {
    return localQuorum_;
}

int Network::globalThreshold() const {
    return globalThreshold_;
}

int Network::replyThreshold() const {
    return replyThreshold_;
}

int Network::representativeForGroup(int groupId) const {
    if (groupId < 0 || groupId >= static_cast<int>(groups_.size())) {
        return -1;
    }
    return groups_[groupId].representativeId;
}

bool Network::nodeBelongsToGroup(int nodeId, int groupId) const {
    const auto it = nodeToGroup_.find(nodeId);
    return it != nodeToGroup_.end() && it->second == groupId;
}

bool Network::isRepresentative(int nodeId) const {
    return std::any_of(groups_.begin(), groups_.end(), [nodeId](const Group &group) {
        return group.representativeId == nodeId;
    });
}

void Network::log(const std::string &text) const {
    std::lock_guard<std::mutex> lock(logMutex_);
    std::cout << "[event=" << std::setw(4) << std::setfill('0') << ++eventNumber_ << "] "
              << text << '\n';
}

void Network::printConfiguration() const {
    log("=== KONFIGURACIJA ===");
    log("n=" + std::to_string(numberOfNodes_) + " | m=" + std::to_string(groupSize_) +
        " | R=" + std::to_string(R_) + " | E=" + std::to_string(E_) +
        " | w=" + std::to_string(w_));
    log("lokalni prag=" + std::to_string(localQuorum_) + " | globalni prag=" +
        std::to_string(globalThreshold_) + " | REPLY prag=" +
        std::to_string(replyThreshold_));
    log("primary=Node " + std::to_string(primaryId_));
    for (const Group &group : groups_) {
        std::string members;
        for (int nodeId : group.members) {
            if (!members.empty()) {
                members += ",";
            }
            members += std::to_string(nodeId);
        }
        log("Grupa " + std::to_string(group.id) + " | predstavnik=Node " +
            std::to_string(group.representativeId) + " | clanovi=" + members);
    }
}

void Network::printFinalState() const {
    log("=== KONACNO STANJE ===");
    for (const auto &node : nodes_) {
        const std::string decision =
            node->decidedValue().empty() ? "-" : node->decidedValue();
        log("Node " + std::to_string(node->id()) + " | grupa=" +
            std::to_string(node->groupId()) + " | faza=" +
            nodePhaseToString(node->phase()) + " | fault=" +
            faultModeToString(node->faultMode()) + " | odluka=" + decision);
    }
}

uint32_t Network::hashText(const std::string &text) const {
    uint32_t hash = 2166136261u;
    for (unsigned char character : text) {
        hash ^= character;
        hash *= 16777619u;
    }
    return hash;
}

std::vector<RingEntry> Network::buildHashRing() const {
    std::vector<RingEntry> ring;
    for (int nodeId = 0; nodeId < numberOfNodes_; ++nodeId) {
        const std::string ip = "10.0.0." + std::to_string(nodeId + 1);
        ring.push_back(RingEntry{nodeId, hashText(ip)});
    }
    std::sort(ring.begin(), ring.end(), [](const RingEntry &left, const RingEntry &right) {
        return left.hash == right.hash ? left.nodeId < right.nodeId : left.hash < right.hash;
    });
    return ring;
}

int Network::clockwiseNode(uint32_t target, const std::vector<RingEntry> &ring) const {
    const auto it = std::find_if(ring.begin(), ring.end(), [target](const RingEntry &entry) {
        return entry.hash >= target;
    });
    return it == ring.end() ? ring.front().nodeId : it->nodeId;
}

void Network::buildTopology() {
    groups_.clear();
    nodeToGroup_.clear();
    const std::vector<RingEntry> ring = buildHashRing();

    const uint32_t primaryTarget =
        hashText(masterIp_ + previousHash_ + std::to_string(view_));
    primaryId_ = clockwiseNode(primaryTarget, ring);

    std::vector<int> orderedNodes;
    const int start = (view_ / numberOfNodes_) % numberOfNodes_;
    for (int offset = 0; offset < numberOfNodes_; ++offset) {
        const int nodeId = ring[(start + offset) % numberOfNodes_].nodeId;
        if (nodeId != primaryId_) {
            orderedNodes.push_back(nodeId);
        }
    }

    for (int groupId = 0; groupId < R_; ++groupId) {
        Group group;
        group.id = groupId;
        for (int position = 0; position < groupSize_; ++position) {
            const int nodeId = orderedNodes[groupId * groupSize_ + position];
            group.members.push_back(nodeId);
            nodeToGroup_[nodeId] = groupId;
        }

        std::vector<RingEntry> groupRing;
        for (const RingEntry &entry : ring) {
            const auto groupIt = nodeToGroup_.find(entry.nodeId);
            if (groupIt != nodeToGroup_.end() && groupIt->second == groupId) {
                groupRing.push_back(entry);
            }
        }

        const uint32_t representativeTarget =
            hashText(masterIp_ + std::to_string(view_) + std::to_string(groupId));
        group.representativeId = clockwiseNode(representativeTarget, groupRing);
        groups_.push_back(group);
    }
}

void Network::receiveClientReply(const Message &message) {
    std::lock_guard<std::mutex> lock(clientMutex_);
    if (message.signatures.size() != 1 || message.signatures.count(message.from) == 0) {
        return;
    }

    auto &senders = clientReplies_[message.value];
    senders.insert(message.from);
    log("CLIENT prima REPLY za " + message.value + " | " + std::to_string(senders.size()) +
        "/" + std::to_string(replyThreshold_));
    if (!clientDecided_ && senders.size() >= static_cast<size_t>(replyThreshold_)) {
        clientDecided_ = true;
        clientValue_ = message.value;
        log("CLIENT POTVRDJUJE KONSENZUS ZA " + clientValue_);
        clientReady_.notify_all();
    }
}

std::vector<int> Network::ordinaryNodeCandidates() const {
    std::vector<int> candidates;
    for (int position = 0; position < groupSize_; ++position) {
        for (const Group &group : groups_) {
            const int nodeId = group.members[position];
            if (nodeId != group.representativeId) {
                candidates.push_back(nodeId);
            }
        }
    }
    return candidates;
}
