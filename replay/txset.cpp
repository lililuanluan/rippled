#include "test/csf/Tx.h"
#include "test/csf/Validation.h"

#include "print.h"
#include "trace.h"
#include "utils.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>

// 现在trace支持获取所有position（=txset）的hash，以及每个position所包含的disputed transaction的hash
// 这里要构造一个由真实position hash映射到csf的transaction set的mapping
std::map<std::string, TxSet>
makePositionSets(replay_trace::TraceData const& trace)
{
    auto const disputedTxs = trace.disputedTransactions();
    auto const disputedByPosition = trace.disputedTxByPosition();

    std::map<std::string, TxSet> positionSets;
    // 如果没有membership记录，但是有多个不同的真实hash（默认有一个全0hash，以及其他真实hash），则直接报错
    if (disputedTxs.empty() && disputedByPosition.size() > 1)
    {
        throw std::runtime_error("multiple distinct positions, but no disputed tx");
    }

    std::string const emptyHash(64, '0');

    for (auto&& [positionHash, txHashes] : disputedByPosition)
    {
        TxSetType txset;
        if (positionHash != emptyHash)
        {
            // 找到这个position包含的disputed
            // transactions在disputedTxs中对应的下标整数，作为csf中整数transaction，构造一个txset
            for (auto&& txHash : txHashes)
            {
                auto it = std::ranges::find(disputedTxs, txHash);
                if (it == disputedTxs.end())
                {
                    throw std::runtime_error("couldn't find disputed transaction: " + txHash);
                }
                auto const syntheticID = static_cast<std::uint32_t>(it - disputedTxs.begin());
                txset.insert(Tx(syntheticID));
            }
        }
        positionSets[positionHash] = txset;
    }
    return positionSets;
}

// 再增加一个从真实的position hash到synthetic的disputed transaction set的TxSet::ID的映射
auto
getSyntheticTxSetId(
    std::map<std::string, TxSet> const& positionSets,
    std::string const& tracePositionHash)
{
    return positionSets.at(tracePositionHash).id();
}

void
check(
    ReplayCollector const& collector,
    replay_trace::TraceData const& trace,
    std::map<std::string, TxSet> const& positionSets)
{
    auto const acceptedLedgers = collector.getAcceptedLedgerById();

    for (auto const& node : trace.nodes)
    {
        std::cout << "--------------------------" << std::endl;
        PeerID const who{node.id};

        auto const expectedAccept = std::ranges::find_if(
            trace.accepts, [&](auto const& accept) { return accept.node == node.id; });
        if (expectedAccept == trace.accepts.end())
            throw std::runtime_error("trace has no accept for node " + std::to_string(node.id));

        auto const actualAccept = collector.accepts.find(who);
        if (actualAccept == collector.accepts.end() || actualAccept->second.empty())
            throw std::runtime_error("cannot find accepted ledger for node " +
                                     std::to_string(node.id));

        Ledger const& acceptedLedger = actualAccept->second.front().ledger;
        if (TxSet::calcID(acceptedLedger.txs()) !=
            getSyntheticTxSetId(positionSets, expectedAccept->txsetHash))
        {
            throw std::runtime_error("accepted txset does not match trace for node " +
                                     std::to_string(node.id));
        }

        std::cout << "[Accepted]\tnode " << node.id << " accepted L" << acceptedLedger.id()
                  << " txs=" << acceptedLedger.txs() << std::endl;

        auto const expectedValidation = std::ranges::find_if(
            trace.validations,
            [&](auto const& validation) { return validation.node == node.id; });
        auto const actualValidation = collector.validationShares.find(who);
        bool const hasValidation =
            actualValidation != collector.validationShares.end() &&
            !actualValidation->second.empty();
        bool const expectsValidation = expectedValidation != trace.validations.end();

        if (hasValidation != expectsValidation)
            throw std::runtime_error("validation presence does not match trace for node " +
                                     std::to_string(node.id));
        if (!hasValidation)
        {
            std::cout << "[Validation]\tnode " << node.id << " didn't share validation"
                      << std::endl;
            continue;
        }

        auto const expectedLedger = std::ranges::find_if(
            trace.accepts,
            [&](auto const& accept) { return accept.ledgerHash == expectedValidation->ledgerHash; });
        if (expectedLedger == trace.accepts.end())
            throw std::runtime_error("trace validation references an unknown ledger");

        Ledger::ID const validatedLedgerID =
            actualValidation->second.front().val.ledgerID();
        auto const validatedLedger = acceptedLedgers.find(validatedLedgerID);
        if (validatedLedger == acceptedLedgers.end())
        {
            throw std::runtime_error("validation references an unavailable ledger for node " +
                                     std::to_string(node.id));
        }
        if (TxSet::calcID(validatedLedger->second.txs()) !=
            getSyntheticTxSetId(positionSets, expectedLedger->txsetHash))
        {
            throw std::runtime_error("validated txset does not match trace for node " +
                                     std::to_string(node.id));
        }

        std::cout << "[Validation]\tnode " << node.id << " validated L" << validatedLedgerID
                  << " txs=" << validatedLedger->second.txs() << std::endl;
    }

    std::cout << "\n *** TxSet replay succeeded: final results match trace *** " << std::endl;
}

void
replay(replay_trace::TraceData& trace)
{
    Sim sim;
    ReplayCollector collector;
    sim.collectors.add(collector);

    auto peers = sim.createGroup(trace.nodes.size());
    configUNL(peers, trace);
    auto const peersById = peerById(peers);

    auto peer = [&](std::uint32_t id) { return getPeerById(peersById, id); };

    // 获取每个position hash对应的txset
    auto positionSets = makePositionSets(trace);

    // 初始化节点的previous round的事件参数，记录见过的txset省得以后再acquire，以及修改openTxs
    for (auto const& n : trace.nodes)
    {
        Peer* p = peer(n.id);
        p->targetLedgers = 0;
        p->fakeSetPreviousRound(std::chrono::milliseconds{n.prevRoundTimeMs}, n.prevProposers);
        for (auto&& [_, txSet] : positionSets)
            p->handle(txSet);
        for (auto&& tx : positionSets[n.initialPositionHash].txs())
        {
            p->openTxs.insert(tx);
        }
    }

    // 这里给所有节点一个假的closetime，因为这里只关心txset
    auto const replayCloseTime = netTimeFromSeconds(trace.nodes.front().prevCloseTime);

    auto const start = sim.scheduler.now();
    auto scheduleAt = [&](std::int64_t atUs, auto&& f) {
        sim.scheduler.at(start + microseconds{atUs}, std::forward<decltype(f)>(f));
    };

    // 注册startround和closeledger事件
    for (auto&& n : trace.nodes)
    {
        // id必须用值捕获，因为是后续执行的，已经出了这个循环了，n会失效
        scheduleAt(n.roundStartUs, [&, id = n.id]() { peer(id)->startRound(); });
        scheduleAt(n.establishUs, [&, id = n.id]() { peer(id)->fakeCloseLedger(replayCloseTime); });
    }

    auto syntheticTxSetIdFor = [&](std::string const& tracePositionHash) {
        return getSyntheticTxSetId(positionSets, tracePositionHash);
    };

    for (auto&& d : trace.deliveries)
    {
        scheduleAt(d.atUs, [&, d]() {
            if (auto const* emitted = collector.findEmittedProposal(
                    PeerID{d.sender}, d.proposalSeq, syntheticTxSetIdFor(d.positionHash)))
            {
                peer(d.receiver)->handle(*emitted);
            }
            else
            {
                std::stringstream ss;
                ss << "txset delivery does not match an emitted proposal: sender=" << d.sender
                   << " receiver=" << d.receiver << " seq=" << d.proposalSeq
                   << " position=" << d.positionHash;
                throw std::runtime_error(ss.str());
            }
        });
    }

    for (auto&& tick : trace.ticks)
    {
        scheduleAt(tick.atUs, [&, tick] {
            // 这里先检查一下已经发送validation的节点数和trace在这个tick上节点观察到的是否匹配
            std::set<PeerID> validatedPeers;
            for (auto&& [sender, shares] : collector.validationShares)
            {
                if (!shares.empty() && sender != PeerID(tick.node))
                {
                    validatedPeers.insert(sender);
                }
            }
            if (validatedPeers.size() < tick.observedValidated)
            {
                throw std::runtime_error("not enough validations sent at this moment");
            }

            // 然后调用timerentryonce，在这之前需要固定proposers finished
            auto* p = peer(tick.node);
            p->setProposersFinishedOverride(tick.observedValidated);
            p->timerEntryOnce();
            p->clearProposersFinishedOverride();
        });
    }

    sim.scheduler.step();

    check(collector, trace, positionSets);
}

int
main(int argc, char** argv)
try
{
    // G53T17
    // seq 9
    fs::path const here = fs::path(__FILE__).parent_path();
    auto trace = replay_trace::loadTrace(here / "G53T17_trace.json", 9);
    // std::cout << trace << std::endl;
    replay(trace);
}
catch (std::exception const& e)
{
    std::cerr << "ERROR: " << e.what() << std::endl;
    return 1;
}
catch (...)
{
    std::cerr << "ERROR: unknown exception" << std::endl;
    return 1;
}
