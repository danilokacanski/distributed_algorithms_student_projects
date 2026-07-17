#ifndef SIMPLE_NBFT_NETWORK_HPP
#define SIMPLE_NBFT_NETWORK_HPP

#include "Message.hpp"
#include "Node.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

struct Group {
    int id = -1;
    std::vector<int> members;
    int representativeId = -1;
};

struct RingEntry {
    int nodeId = -1;
    uint32_t hash = 0;
};

class Network {
  public:
    Network(int numberOfNodes, int groupSize);
    ~Network();

    Network(const Network &) = delete;
    Network &operator=(const Network &) = delete;

    void initialize();
    void configureScenario(const std::string &scenario, int byzantineCount);
    void start();
    void stop();
    bool runConsensus(const std::string &value, int timeoutMs);

    void send(Message message);
    void broadcastToReplicas(Message message);
    void broadcastToGroup(Message message, int groupId, int excludedNode = -1);
    void broadcastToRepresentatives(Message message);
    void broadcastToOtherRepresentatives(Message message, int excludedGroup);

    int primaryId() const;
    int groupSize() const;
    int E() const;
    int localQuorum() const;
    int globalThreshold() const;
    int replyThreshold() const;
    int representativeForGroup(int groupId) const;
    bool nodeBelongsToGroup(int nodeId, int groupId) const;
    bool isRepresentative(int nodeId) const;

    void log(const std::string &text) const;
    void printConfiguration() const;
    void printFinalState() const;

  private:
    int numberOfNodes_;
    int groupSize_;
    int view_ = 0;
    int sequenceNumber_ = 1;
    int primaryId_ = -1;
    int E_;
    int R_;
    int w_;
    int localQuorum_;
    int globalThreshold_;
    int replyThreshold_;

    std::string masterIp_ = "10.0.0.1";
    std::string previousHash_ = "GENESIS_BLOCK_HASH";

    std::vector<std::unique_ptr<Node>> nodes_;
    std::vector<Group> groups_;
    std::map<int, int> nodeToGroup_;

    std::atomic<int> nextMessageId_{1};
    mutable std::mutex logMutex_;
    mutable int eventNumber_ = 0;

    mutable std::mutex clientMutex_;
    std::condition_variable clientReady_;
    std::map<std::string, std::set<int>> clientReplies_;
    bool clientDecided_ = false;
    std::string clientValue_;

    uint32_t hashText(const std::string &text) const;
    std::vector<RingEntry> buildHashRing() const;
    int clockwiseNode(uint32_t target, const std::vector<RingEntry> &ring) const;
    void buildTopology();
    void receiveClientReply(const Message &message);
    std::vector<int> ordinaryNodeCandidates() const;
};

#endif
