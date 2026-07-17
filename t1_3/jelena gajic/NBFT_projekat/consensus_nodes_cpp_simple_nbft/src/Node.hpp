#ifndef SIMPLE_NBFT_NODE_HPP
#define SIMPLE_NBFT_NODE_HPP

#include "Message.hpp"

#include <condition_variable>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>

class Network;

enum class FaultMode {
    HONEST,
    WRONG_VALUE,
    LOW_SIGNATURES,
    SILENT
};

enum class NodePhase {
    IDLE,
    PRE_PREPARE1,
    IN_PREPARE1,
    IN_PREPARE2,
    OUT_PREPARE,
    COMMIT,
    PRE_PREPARE2,
    DECIDED
};

std::string faultModeToString(FaultMode mode);
std::string nodePhaseToString(NodePhase phase);

class Node {
  public:
    Node(int id, Network &network);
    ~Node();

    Node(const Node &) = delete;
    Node &operator=(const Node &) = delete;

    void configureTopology(int groupId, bool primary, bool representative);
    void setFaultMode(FaultMode mode);
    void reset();

    void start();
    void stop();
    void enqueue(Message message);

    int id() const;
    int groupId() const;
    bool isPrimary() const;
    bool isRepresentative() const;
    FaultMode faultMode() const;
    NodePhase phase() const;
    const std::string &decidedValue() const;

  private:
    int id_;
    Network &network_;
    int groupId_ = -1;
    bool primary_ = false;
    bool representative_ = false;
    FaultMode faultMode_ = FaultMode::HONEST;
    NodePhase phase_ = NodePhase::IDLE;

    std::thread worker_;
    std::mutex inboxMutex_;
    std::condition_variable inboxReady_;
    std::queue<Message> inbox_;
    bool running_ = false;

    int view_ = 0;
    int sequenceNumber_ = 1;
    std::string proposedValue_;
    std::string decidedValue_;

    std::map<std::string, std::set<int>> localVotes_;
    bool localCertificateSent_ = false;
    bool nodeDecisionSent_ = false;

    std::map<int, int> normalGroupWeights_;
    std::map<int, std::set<int>> abnormalNodesByGroup_;
    bool commitSent_ = false;
    bool finalBroadcastSent_ = false;
    bool replySent_ = false;

    void run();
    void handle(const Message &message);
    void handleClientRequest(const Message &message);
    void handlePrePrepare1(const Message &message);
    void handleInPrepare1(const Message &message);
    void handleInPrepare2(const Message &message);
    void handleOutPrepare(const Message &message);
    void handleCommit(const Message &message);
    void handlePrePrepare2(const Message &message);

    void sendNormalOutPrepare(const Message &localCertificate);
    void sendNodeDecision(const std::string &reason);
    void trySendCommit();
    void decideAndReply(const std::string &value);
    bool validGroupSignatures(const std::set<int> &signatures, int groupId) const;
};

#endif
