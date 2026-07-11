#ifndef UTILS_H
#define UTILS_H

#include "test/csf.h"
#include "test/csf/Peer.h"
#include "test/csf/Proposal.h"
#include "test/csf/SimTime.h"
#include "test/csf/Validation.h"
#include "test/csf/events.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <stdexcept>

namespace fs = std::filesystem;
using namespace xrpl::test::csf;
using namespace std::chrono;

struct ReplayCollector
{
    std::map<PeerID, std::vector<Share<Proposal>>> proposalShares;
    std::map<PeerID, std::vector<Share<Validation>>> validationShares;
    std::map<PeerID, std::vector<AcceptLedger>> accepts;

    template <typename Event>
    void
    on(PeerID, SimTime, Event const&)
    {
    }

    void
    on(PeerID who, SimTime, Share<Proposal> const& e)
    {
        proposalShares[who].push_back(e);
    }

    void
    on(PeerID who, SimTime, Share<Validation> const& e)
    {
        validationShares[who].push_back(e);
    }

    void
    on(PeerID who, SimTime, AcceptLedger const& e)
    {
        accepts[who].push_back(e);
    }

    std::map<Ledger::ID, Ledger>
    getAcceptedLedgerById() const
    {
        std::map<Ledger::ID, Ledger> ledgers;
        for (auto&& [_, acc] : accepts)
        {
            for (auto&& accepted : acc)
            {
                ledgers[accepted.ledger.id()] = accepted.ledger;
            }
        }
        return ledgers;
    }

    Proposal const*
    findEmittedProposal(PeerID sender, std::uint32_t proposalSeq, TxSet::ID const& position) const
    {
        auto it = proposalShares.find(sender);
        if (it == proposalShares.end())
            return nullptr;

        // 在sender发送的proposal中找到seq和position匹配的proposal
        for (auto&& shared : it->second)
        {
            auto const& p = shared.val;
            if (p.proposeSeq() == proposalSeq && p.position() == position)
            {
                return &p;
            }
        }
        return nullptr;
    }

    Proposal const*
    findEmittedProposal(
        PeerID sender,
        std::uint32_t proposalSeq,
        TxSet::ID const& position,
        xrpl::NetClock::time_point const& closeTime) const
    {
        auto it = proposalShares.find(sender);
        if (it == proposalShares.end())
            return nullptr;

        for (auto const& shared : it->second)
        {
            auto const& p = shared.val;
            if (p.proposeSeq() == proposalSeq && p.position() == position &&
                p.closeTime() == closeTime)
            {
                return &p;
            }
        }
        return nullptr;
    }
};

// 获取一个用id做下标的Peer*指针数组
std::vector<Peer*>
peerById(PeerGroup const& peers)
{
    std::vector<Peer*> out(peers.size(), nullptr);
    for (auto* p : peers)
    {
        auto const id = static_cast<std::uint32_t>(p->id);
        if (id >= out.size())
        {
            throw std::runtime_error("peerById: peer id out of bound");
        }
        out[id] = p;
    }
    return out;
}

Peer*
getPeerById(std::vector<Peer*> peerByIdRes, std::uint32_t id)
{
    if (id >= peerByIdRes.size() || !peerByIdRes[id])
        throw std::runtime_error("peer: id not available in peers");
    return peerByIdRes[id];
}

void
configUNL(PeerGroup& peers, replay_trace::TraceData const& trace)
{
    auto const& unl = trace.unl;
    auto p = peerById(peers);
    for (auto&& [node, trusted] : unl)
    {
        for (auto&& t : trusted)
        {
            auto tp = getPeerById(p, t);
            getPeerById(p, node)->trust(*tp);
        }
    }
}

bool
isByzz(replay_trace::TraceData const& trace, std::uint32_t node)
{
    return std::ranges::find(trace.byzantineNodes, node) != trace.byzantineNodes.end();
}

xrpl::NetClock::time_point
netTimeFromSeconds(std::int64_t sec)
{
    return xrpl::NetClock::time_point{std::chrono::seconds(sec)};
}

#endif  // !UTILS_H
