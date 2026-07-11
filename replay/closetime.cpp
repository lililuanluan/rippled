#include "test/csf/Tx.h"
#include "test/csf/Validation.h"

#include "print.h"
#include "trace.h"
#include "utils.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

// All nodes start from the same previous ledger and replay only the target seq's consensus round.
Ledger
makeCommonLedger(Sim& sim, std::uint32_t targetSeq, std::int64_t prevCloseTime)
{
    Ledger ledger{Ledger::MakeGenesis{}};
    if (targetSeq <= 1)
        return ledger;

    auto close = netTimeFromSeconds(prevCloseTime - static_cast<std::int64_t>(targetSeq - 1) * 10);
    for (std::uint32_t seq = 1; seq < targetSeq; ++seq)
    {
        if (seq + 1 == targetSeq)
            close = netTimeFromSeconds(prevCloseTime);
        else
            close += std::chrono::seconds{10};

        ledger = sim.oracle.accept(ledger, TxSetType{}, xrpl::kLedgerGenesisTimeResolution, close);
    }
    return ledger;
}

// CloseTime replay only needs distinct real positions to remain distinct in CSF.
std::map<std::string, TxSet>
makePositionSets(replay_trace::TraceData const& trace)
{
    std::map<std::string, TxSet> positionSets;
    std::string const emptyHash(64, '0');
    std::uint32_t nextSyntheticTx = 0;

    for (auto const& positionHash : trace.positionHashes())
    {
        TxSetType txs;
        if (positionHash != emptyHash)
            txs.insert(Tx{nextSyntheticTx++});
        positionSets.emplace(positionHash, TxSet{std::move(txs)});
    }
    return positionSets;
}

auto
getSyntheticTxSetId(
    std::map<std::string, TxSet> const& positionSets,
    std::string const& tracePositionHash)
{
    return positionSets.at(tracePositionHash).id();
}

// Real ledger hashes differ from CSF ledger IDs, so only compare whether nodes end up on the same
// ledger branches.
void
checkBranches(
    std::vector<replay_trace::TraceNode> const& nodes,
    std::map<std::uint32_t, Ledger::ID> const& actual,
    std::map<std::uint32_t, std::string> const& expected,
    std::string_view label)
{
    for (std::size_t i = 0; i < nodes.size(); ++i)
    {
        for (std::size_t j = i + 1; j < nodes.size(); ++j)
        {
            auto const lhs = nodes[i].id;
            auto const rhs = nodes[j].id;
            bool const sameActual = actual.at(lhs) == actual.at(rhs);
            bool const sameExpected = expected.at(lhs) == expected.at(rhs);
            if (sameActual != sameExpected)
            {
                throw std::runtime_error(
                    std::string{label} + " branch does not match trace between node " +
                    std::to_string(lhs) + " and node " + std::to_string(rhs));
            }
        }
    }
}

void
check(ReplayCollector const& collector, replay_trace::TraceData const& trace)
{
    auto const acceptedLedgers = collector.getAcceptedLedgerById();
    std::map<std::uint32_t, Ledger::ID> actualAccepts;
    std::map<std::uint32_t, Ledger::ID> actualValidations;
    std::map<std::uint32_t, std::string> expectedAccepts;
    std::map<std::uint32_t, std::string> expectedValidations;

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
            throw std::runtime_error(
                "cannot find accepted ledger for node " + std::to_string(node.id));

        Ledger const& acceptedLedger = actualAccept->second.front().ledger;
        actualAccepts[node.id] = acceptedLedger.id();
        expectedAccepts[node.id] = expectedAccept->ledgerHash;

        auto const acceptedCloseTime = std::chrono::duration_cast<std::chrono::seconds>(
                                           acceptedLedger.closeTime().time_since_epoch())
                                           .count();
        std::cout << "[Accepted]\tnode " << node.id << " accepted L" << acceptedLedger.id()
                  << " close_time=" << acceptedCloseTime << " close_agree=" << std::boolalpha
                  << acceptedLedger.closeAgree() << std::noboolalpha << std::endl;

        auto const expectedValidation = std::ranges::find_if(
            trace.validations, [&](auto const& validation) { return validation.node == node.id; });
        auto const actualValidation = collector.validationShares.find(who);
        bool const hasValidation = actualValidation != collector.validationShares.end() &&
            !actualValidation->second.empty();
        bool const expectsValidation = expectedValidation != trace.validations.end();

        if (hasValidation != expectsValidation)
            throw std::runtime_error(
                "validation presence does not match trace for node " + std::to_string(node.id));

        if (!hasValidation)
        {
            actualValidations[node.id] = Ledger::ID{0};
            expectedValidations[node.id] = {};
            std::cout << "[Validation]\tnode " << node.id << " didn't share validation"
                      << std::endl;
            continue;
        }

        Ledger::ID const validatedLedgerID = actualValidation->second.front().val.ledgerID();
        auto const validatedLedger = acceptedLedgers.find(validatedLedgerID);
        if (validatedLedger == acceptedLedgers.end())
        {
            throw std::runtime_error(
                "validation references an unavailable ledger for node " + std::to_string(node.id));
        }

        actualValidations[node.id] = validatedLedgerID;
        expectedValidations[node.id] = expectedValidation->ledgerHash;
        auto const validatedCloseTime = std::chrono::duration_cast<std::chrono::seconds>(
                                            validatedLedger->second.closeTime().time_since_epoch())
                                            .count();
        std::cout << "[Validation]\tnode " << node.id << " validated L" << validatedLedgerID
                  << " close_time=" << validatedCloseTime << " close_agree=" << std::boolalpha
                  << validatedLedger->second.closeAgree() << std::noboolalpha << std::endl;
    }

    checkBranches(trace.nodes, actualAccepts, expectedAccepts, "accept");
    checkBranches(trace.nodes, actualValidations, expectedValidations, "validation");

    std::cout << "\n*** CloseTime replay succeeded: final results match trace *** " << std::endl;
}

void
replay(replay_trace::TraceData const& trace)
{
    Sim sim;
    ReplayCollector collector;
    sim.collectors.add(collector);

    auto peers = sim.createGroup(trace.nodes.size());
    configUNL(peers, trace);
    auto const peersById = peerById(peers);
    auto peer = [&](std::uint32_t id) { return getPeerById(peersById, id); };

    Ledger const commonLedger =
        makeCommonLedger(sim, trace.targetSeq, trace.nodes.front().prevCloseTime);
    auto const positionSets = makePositionSets(trace);

    for (auto const& node : trace.nodes)
    {
        Peer* p = peer(node.id);
        p->targetLedgers = 0;
        p->lastClosedLedger = commonLedger;
        p->fullyValidatedLedger = commonLedger;
        p->ledgers[commonLedger.id()] = commonLedger;
        p->fakeSetPreviousRound(
            std::chrono::milliseconds{node.prevRoundTimeMs}, node.prevProposers);

        for (auto const& [_, txSet] : positionSets)
            p->handle(txSet);
        for (auto const& tx : positionSets.at(node.initialPositionHash).txs())
            p->openTxs.insert(tx);
    }

    auto const start = sim.scheduler.now();
    auto scheduleAt = [&](std::int64_t atUs, auto&& f) {
        sim.scheduler.at(start + std::chrono::microseconds{atUs}, std::forward<decltype(f)>(f));
    };

    for (auto const& node : trace.nodes)
    {
        scheduleAt(node.roundStartUs, [&, id = node.id]() { peer(id)->startRound(); });
        scheduleAt(node.establishUs, [&, node]() {
            peer(node.id)->fakeCloseLedger(netTimeFromSeconds(node.initialCloseTime));
        });
    }

    auto syntheticTxSetIdFor = [&](std::string const& tracePositionHash) {
        return getSyntheticTxSetId(positionSets, tracePositionHash);
    };

    constexpr bool allowByzantineProposalInjection = true;
    std::size_t byzantineInjectedProposals = 0;
    for (auto const& delivery : trace.deliveries)
    {
        scheduleAt(delivery.atUs, [&, delivery]() {
            auto const expectedPosition = syntheticTxSetIdFor(delivery.positionHash);
            auto const expectedCloseTime = netTimeFromSeconds(delivery.closeTime);

            if (auto const* emitted = collector.findEmittedProposal(
                    PeerID{delivery.sender},
                    delivery.proposalSeq,
                    expectedPosition,
                    expectedCloseTime))
            {
                peer(delivery.receiver)->handle(*emitted);
                return;
            }

            if (allowByzantineProposalInjection && isByzz(trace, delivery.sender))
            {
                Proposal injected(
                    commonLedger.id(),
                    delivery.proposalSeq,
                    expectedPosition,
                    expectedCloseTime,
                    peer(delivery.receiver)->now(),
                    PeerID{delivery.sender});
                peer(delivery.receiver)->handle(injected);
                ++byzantineInjectedProposals;
                return;
            }

            std::stringstream message;
            message << "honest proposal was not emitted: sender=" << delivery.sender
                    << " receiver=" << delivery.receiver << " seq=" << delivery.proposalSeq
                    << " position=" << delivery.positionHash
                    << " close_time=" << delivery.closeTime;
            throw std::runtime_error(message.str());
        });
    }

    auto deliverAvailableValidationsTo = [&](std::uint32_t receiver) {
        auto const acceptedLedgers = collector.getAcceptedLedgerById();
        Peer* p = peer(receiver);
        for (auto const& [sender, shares] : collector.validationShares)
        {
            if (sender == PeerID{receiver})
                continue;

            for (auto const& share : shares)
            {
                auto const ledger = acceptedLedgers.find(share.val.ledgerID());
                if (ledger != acceptedLedgers.end())
                    p->ledgers[ledger->first] = ledger->second;
                p->handle(share.val);
            }
        }
    };

    for (auto const& tick : trace.ticks)
    {
        scheduleAt(tick.atUs, [&, tick]() {
            deliverAvailableValidationsTo(tick.node);
            Peer* p = peer(tick.node);
            p->setProposersFinishedOverride(tick.observedValidated);
            p->timerEntryOnce();
            p->clearProposersFinishedOverride();
        });
    }

    sim.scheduler.step();

    for (auto const& node : trace.nodes)
        deliverAvailableValidationsTo(node.id);

    std::cout << "Byzantine injected proposals: " << byzantineInjectedProposals << std::endl;
    check(collector, trace);
}

int
main(int argc, char** argv)
try
{
    // G12T14
    // seq 7
    fs::path const here = fs::path(__FILE__).parent_path();
    auto trace = replay_trace::loadTrace(here / "G12T14_trace.json", 7);
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
