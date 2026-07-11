
#ifndef TRACE_H
#define TRACE_H

#include "json.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
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
    std::string txHash;  // 就是positionHash
    bool present;        // txHash 是否在 txsetHash 对应的transaction set中
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

    std::set<std::string>
    positionHashes() const
    {
        // 返回所有见过的position的hash
        std::set<std::string> hashes;
        for (auto const& n : nodes)
            hashes.insert(n.initialPositionHash);
        for (auto const& d : deliveries)
            hashes.insert(d.positionHash);
        for (auto const& acc : accepts)
            hashes.insert(acc.txsetHash);
        for (auto const& mem : txsetMemberships)
            hashes.insert(mem.txsetHash);

        return hashes;
    }

    // 返回txsetmembership中所有的txset的集合
    // 这里只记录的是disputed（非disputed一定在每个position中）
    std::vector<std::string>
    disputedTransactions() const
    {
        std::set<std::string> disputes;
        for (auto&& mem : txsetMemberships)
        {
            disputes.insert(mem.txHash);
        }
        return {disputes.begin(), disputes.end()};
    }

    // 获取每个positionash都包含哪些disputed transaction的hash
    auto
    disputedTxByPosition() const
    {
        std::map<std::string, std::set<std::string>> res;  // position hash -> {tx hash}
        for (auto const& h : positionHashes())
        {
            res[h] = {};
        }

        for (auto&& mem : txsetMemberships)
        {
            if (mem.present)
            {
                res[mem.txsetHash].insert(mem.txHash);
            }
        }
        return res;
    }
};

namespace fs = std::filesystem;

TraceData
loadTrace(std::string const& tracePath, std::uint32_t targetSeq)
{
    std::ifstream input(tracePath);
    if (!input)
        throw std::runtime_error("cannot open trace file: " + tracePath);
    json::Reader reader;
    json::Value root;
    reader.parse(input, root);

    TraceData trace;
    trace.targetSeq = targetSeq;

    for (auto&& node : get(root, "byzantine_nodes"))
    {
        trace.byzantineNodes.push_back(node.asUInt());
    }

    for (auto&& u : get(root, "unl"))
    {
        // u: node i, trusted []
        std::vector<std::uint32_t> trusted;
        auto const node = get(u, "node");
        for (auto&& t : get(u, "trusted"))
        {
            trusted.push_back(t.asUInt());
        }
        trace.unl.emplace_back(node.asUInt(), std::move(trusted));
    }

    auto match_seq = [&](auto&& entry) { return get<std::uint32_t>(entry, "seq") == targetSeq; };

    for (auto&& node : get(root, "rounds"))
    {
        if (match_seq(node))
        {
            trace.nodes.emplace_back(
                get<std::uint32_t>(node, "node"),
                get<std::int64_t>(node, "round_start_us"),
                get<std::int64_t>(node, "establish_us"),
                get<std::uint32_t>(node, "prev_round_time_ms"),
                get<std::uint32_t>(node, "prev_proposers"),
                get<std::string>(node, "initial_position_hash"),
                get<std::int64_t>(node, "initial_close_time"),
                get<std::int64_t>(node, "prev_close_time"));
        }
    }

    for (auto&& d : get(root, "proposal_deliveries"))
    {
        if (match_seq(d))
        {
            trace.deliveries.emplace_back(
                get<std::int64_t>(d, "at_us"),
                get<std::uint32_t>(d, "sender"),
                get<std::uint32_t>(d, "receiver"),
                get<std::uint32_t>(d, "proposal_seq"),
                get<std::string>(d, "position_hash"),
                get<std::int64_t>(d, "close_time"));
        }
    }

    for (auto&& m : get(root, "txset_memberships"))
    {
        if (match_seq(m))
        {
            trace.txsetMemberships.emplace_back(
                get<std::string>(m, "txset_hash"),
                get<std::string>(m, "tx_hash"),
                get<bool>(m, "present"));
        }
    }

    for (auto&& t : get(root, "timer_ticks"))
    {
        if (match_seq(t))
        {
            trace.ticks.emplace_back(
                get<std::int64_t>(t, "at_us"),
                get<std::uint32_t>(t, "node"),
                get<std::uint32_t>(t, "observed_validated"));
        }
    }

    for (auto&& acc : get(root, "accepts"))
    {
        if (match_seq(acc))
        {
            trace.accepts.emplace_back(
                get<std::uint32_t>(acc, "node"),
                get<std::string>(acc, "ledger_hash"),
                get<std::string>(acc, "txset_hash"));
        }
    }

    for (auto&& v : get(root, "validations"))
    {
        if (match_seq(v))
        {
            trace.validations.emplace_back(
                get<std::uint32_t>(v, "node"), get<std::string>(v, "ledger_hash"));
        }
    }
    return trace;
}
}  // namespace replay_trace

#endif  // !TRACE_H
