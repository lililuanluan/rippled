
#ifndef TRACE_H
#define TRACE_H

#include "json.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <utility>
#include <vector>

namespace replay_trace {

// 一个节点的初始状态配置
struct TraceNode
{
    std::uint32_t id;               // id
    std::int64_t roundStartUs;      // 进入open phase的相对时间
    std::int64_t establishUs;       // 进入establish phase的相对时间
    std::uint32_t prevRoundTimeMs;  // 上一轮用时
    std::uint32_t prevProposers;    // 上一轮的proposer数，这两个传给setpreviousround
    std::string
        initialPositionHash;  // 节点进入establish时proposal里的txset，将这个hash插入openTxs，这样startRound()->closeLedger()之后就会propose这个。注意position=txset，不包含closetime
    std::int64_t initialCloseTime;  // 节点进入establish时proposal里的closetime
    std::int64_t prevCloseTime;     // 用于构建上一个ledger，ripple如果在closetime
                                    // consensus失败到0，则会用prevCloseTime+1作为accept的closetime
};

// 一条proposal的接收事件
struct ProposalDelivery
{
    std::int64_t atUs;  // 消息送达的相对时间
    std::uint32_t sender;
    std::uint32_t receiver;
    std::uint32_t proposalSeq;  // proposal的sequence
                                // number，这个用来在已经发送的proposal中查找用的，找不到就失败
    // 后面这俩是 txset + closetime 这两个在proposal中的信息
    std::string positionHash;
    std::int64_t closeTime;
};

// 一次节点本地的 heartbeat事件，调用timerEntryOnce
struct TimerTick
{
    std::int64_t atUs;   // 时间点
    std::uint32_t node;  // 谁的tick
    std::uint32_t
        observedValidated;  // UNL中，有多少节点已经在这个ledger之后工作（即已经发送了这个ledger的validation了）
};

struct TxSetMembership
{
    std::string txsetHash;
    std::string txHash;
    bool present;  // txHash 是否在 txsetHash 对应的transaction set中
};

// 用于最后校验的Oracle
struct Accept
{
    // 之前用TxSetMembership来记录映射，后面构造一个 fake tx -> tx的映射
    // 这里就是通过 fake tx -> tx -> txsetHash -> ledgerHash 的映射，将最终输出的fake
    // accept映射到真实的hash然后检查分布是否符合
    std::uint32_t node;
    std::string ledgerHash;
    std::string txsetHash;
};

// Oracle
struct Validation
{
    std::uint32_t node;
    std::string ledgerHash;
};

struct Unl
{
    std::uint32_t node;
    std::vector<std::uint32_t> trusted;  // 某个node的UNL
};

struct TraceData
{
    std::uint32_t targetSeq;  // 要replay哪个ledger seq
    std::vector<std::uint32_t> byzantineNodes;
    std::vector<Unl> unl;
    std::vector<TraceNode> nodes;
    std::vector<ProposalDelivery> deliveries;
    std::vector<TxSetMembership> txsetMemberships;
    std::vector<TimerTick> ticks;  // 节点的timerEntry事件
    std::vector<Accept> accepts;
    std::vector<Validation> validations;
};

namespace fs = std::filesystem;

TraceData
loadTrace(std::string const& tracePath)
{
    std::ifstream input(tracePath);
    json::Reader reader;
    json::Value root;
    reader.parse(input, root);

    // 获取replay字段，这是整理好的用于replay的事件
    auto const& replay = get(root, "replay");
    // std::cout << replay << std::endl;

    auto const& nodes = get(replay, "nodes");
    std::vector<TraceNode> traceNodes;
    for (auto&& node : nodes)
    {
        traceNodes.emplace_back(
            get<std::uint32_t>(node, "node"),
            get<std::int64_t>(node, "round_start_us"),
            get<std::int64_t>(node, "establish_us"),
            get<std::uint32_t>(node, "prev_round_time_ms"),
            get<std::uint32_t>(node, "prev_proposers"),
            get<bool>(node, "initial_has_target"),
            get<bool>(node, "expected_has_target"));
    }

    std::vector<ProposalDelivery> traceDeliveries;

    auto const& deliveries = get(replay, "deliveries");
    for (auto&& d : deliveries)
    {
        traceDeliveries.emplace_back(
            get<std::int64_t>(d, "at_us"),
            get<std::uint32_t>(d, "sender"),
            get<std::uint32_t>(d, "receiver"),
            get<std::uint32_t>(d, "proposal_seq"),
            get<bool>(d, "has_target"));
    }

    std::vector<ExpectedProposal> traceExpectedProposals;
    auto const& expProposals = get(replay, "expected_proposals");
    for (auto&& p : expProposals)
    {
        traceExpectedProposals.emplace_back(
            get<std::uint32_t>(p, "sender"),
            get<std::uint32_t>(p, "proposal_seq"),
            get<bool>(p, "has_target"));
    }

    std::vector<TimerTick> traceTimerTicks;
    auto const& ticks = get(replay, "ticks");
    for (auto&& t : ticks)
    {
        traceTimerTicks.emplace_back(
            get<std::int64_t>(t, "at_us"),
            get<std::uint32_t>(t, "node"),
            get<std::uint32_t>(t, "observed_validated"));
    }

    return TraceData{
        std::move(traceNodes),
        std::move(traceDeliveries),
        std::move(traceExpectedProposals),
        std::move(traceTimerTicks)};
}
}  // namespace replay_trace

#endif  // !TRACE_H
