#ifndef SIMPLE_NBFT_MESSAGE_HPP
#define SIMPLE_NBFT_MESSAGE_HPP

#include <set>
#include <string>

inline constexpr int CLIENT_ID = -1;

enum class MessageType {
    CLIENT_REQUEST,
    PRE_PREPARE1,
    IN_PREPARE1,
    IN_PREPARE2,
    OUT_PREPARE,
    COMMIT,
    PRE_PREPARE2,
    REPLY
};

inline std::string messageTypeToString(MessageType type) {
    switch (type) {
    case MessageType::CLIENT_REQUEST:
        return "CLIENT_REQUEST";
    case MessageType::PRE_PREPARE1:
        return "PRE_PREPARE1";
    case MessageType::IN_PREPARE1:
        return "IN_PREPARE1";
    case MessageType::IN_PREPARE2:
        return "IN_PREPARE2";
    case MessageType::OUT_PREPARE:
        return "OUT_PREPARE";
    case MessageType::COMMIT:
        return "COMMIT";
    case MessageType::PRE_PREPARE2:
        return "PRE_PREPARE2";
    case MessageType::REPLY:
        return "REPLY";
    }
    return "UNKNOWN";
}


struct Message {
    int id = 0;
    MessageType type = MessageType::CLIENT_REQUEST;
    int from = CLIENT_ID;
    int to = CLIENT_ID;
    int view = 0;
    int sequenceNumber = 1;
    int groupId = -1;
    std::string value;
    std::set<int> signatures;
    int voteWeight = 0;
    bool nodeDecision = false;
};

#endif
