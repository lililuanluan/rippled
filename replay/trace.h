
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

// Initial state configuration for a node.
struct TraceNode
{
    std::uint32_t id;               // ID
    std::int64_t roundStartUs;      // Relative time when entering the open phase
    std::int64_t establishUs;       // Relative time when entering the establish phase
    std::uint32_t prevRoundTimeMs;  // Duration of the previous round
    std::uint32_t prevProposers;    // Previous-round proposer count; these two values are passed to
                                    // setPreviousRound
    std::string initialPositionHash;  // Txset in the proposal when the node enters establish.
                                      // Insert this hash into openTxs so it is proposed after
                                      // startRound()->closeLedger(). Position = txset and does not
                                      // include close time.
    std::int64_t initialCloseTime;    // Close time in the proposal when entering establish
    std::int64_t prevCloseTime;       // Used to build the previous ledger. If Ripple's close-time
                                      // consensus fails to zero, prevCloseTime + 1 is used as the
                                      // accepted close time.
};

// A proposal receipt event.
struct ProposalDelivery
{
    std::int64_t atUs;  // Relative message delivery time
    std::uint32_t sender;
    std::uint32_t receiver;
    std::uint32_t proposalSeq;  // Proposal sequence number, used to find an already emitted
                                // proposal; fail if it cannot be found
    // The following two fields are the txset and close-time information in the proposal.
    std::string positionHash;
    std::int64_t closeTime;
};

// A node-local heartbeat event that calls timerEntryOnce.
struct TimerTick
{
    std::int64_t atUs;                // Time point
    std::uint32_t node;               // Node whose tick this is
    std::uint32_t observedValidated;  // Number of UNL nodes already working beyond this ledger,
                                      // meaning they have sent its validation
};

struct TxSetMembership
{
    std::string txsetHash;
    std::string txHash;  // The positionHash
    bool present;        // Whether txHash is in the transaction set corresponding to txsetHash
};

// Oracle used for final verification.
struct Accept
{
    // TxSetMembership previously recorded the mapping, then a fake tx -> tx mapping is constructed.
    // The fake tx -> tx -> txsetHash -> ledgerHash mapping converts the final fake accepts
    // to real hashes and checks whether their distribution matches.
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
    std::vector<std::uint32_t> trusted;  // A node's UNL
};

struct TraceData
{
    std::uint32_t targetSeq;  // Ledger seq to replay
    std::vector<std::uint32_t> byzantineNodes;
    std::vector<Unl> unl;
    std::vector<TraceNode> nodes;
    std::vector<ProposalDelivery> deliveries;
    std::vector<TxSetMembership> txsetMemberships;
    std::vector<TimerTick> ticks;  // Node timerEntry events
    std::vector<Accept> accepts;
    std::vector<Validation> validations;

    std::set<std::string>
    positionHashes() const
    {
        // Return the hashes of all observed positions.
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

    // Return the set of all txsets in txsetMemberships.
    // Only disputed transactions are recorded here; non-disputed ones are present in every
    // position.
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

    // Get the disputed transaction hashes contained in each position hash.
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
