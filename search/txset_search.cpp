#include <test/csf/Peer.h>
#include <test/csf/Sim.h>
#include <test/csf/Tx.h>
#include <test/csf/Validation.h>
#include <test/csf/events.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

using namespace xrpl::test::csf;
using namespace std::chrono;

namespace {

constexpr std::uint32_t kNodes = 7;
constexpr std::uint32_t kTicksPerNode = 4;
constexpr std::uint32_t kQuorum = 6;
constexpr std::array<int, 4> kBaseTicksMs{3000, 4000, 19000, 20000};
constexpr std::array<int, 5> kPrevChoicesMs{3000, 4000, 5000, 6000, 8000};
constexpr std::int64_t kCloseLowSec = 1004;
constexpr std::int64_t kCloseHighSec = 1005;
constexpr std::size_t kRlFeatures = 12;
constexpr std::size_t kRlHidden = 16;

struct SearchCollector
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
    acceptedLedgers() const
    {
        std::map<Ledger::ID, Ledger> out;
        for (auto const& [_, rows] : accepts)
        {
            for (auto const& row : rows)
                out[row.ledger.id()] = row.ledger;
        }
        return out;
    }
};

struct Candidate
{
    std::array<bool, kNodes> initialYes{};
    std::array<std::int64_t, kNodes> initialCloseTimeSec{};
    std::array<std::int64_t, kNodes> establishUs{};
    std::array<std::uint32_t, kNodes> prevRoundMs{};
    std::array<std::array<std::int64_t, kTicksPerNode>, kNodes> tickUs{};
    std::array<std::array<std::uint8_t, kTicksPerNode>, kNodes> deliveryMask{};
};

struct RunResult
{
    bool violation = false;
    double score = 0;
    std::uint32_t acceptedCount = 0;
    std::uint32_t validationCount = 0;
    std::uint32_t maxAcceptedVotes = 0;
    std::uint32_t maxValidationVotes = 0;
    std::uint32_t requestedProposalDeliveries = 0;
    std::uint32_t deliveredProposals = 0;
    std::uint32_t missedProposalDeliveries = 0;
    std::map<std::string, std::vector<std::uint32_t>> acceptedSides;
    std::map<std::string, std::vector<std::uint32_t>> validationSides;
    Candidate candidate;
};

struct Policy
{
    std::array<double, kNodes> initialYesProb{};
    std::array<double, kNodes> closeTimeHighProb{};
    std::array<double, kNodes> establishMeanMs{};
    std::array<double, kNodes> establishStdMs{};
    std::array<std::array<double, kPrevChoicesMs.size()>, kNodes> prevRoundProb{};
    std::array<std::array<double, kTicksPerNode>, kNodes> tickMeanMs{};
    std::array<std::array<double, kTicksPerNode>, kNodes> tickStdMs{};
    std::array<std::array<std::array<double, kNodes>, kTicksPerNode>, kNodes> deliveryProb{};
};

struct RlDecision
{
    std::array<double, kRlFeatures> features{};
    bool action = false;
    double probability = 0.0;
    bool initialAction = false;
};

struct RlPolicy
{
    std::array<double, kRlFeatures> initialWeights{};
    std::array<double, kRlFeatures> deliveryWeights{};
    std::array<std::array<double, kRlFeatures>, kRlHidden> deliveryW1{};
    std::array<double, kRlHidden> deliveryB1{};
    std::array<double, kRlHidden> deliveryW2{};
    double deliveryB2 = 0.0;
    double baseline = 0.0;
    double learningRate = 0.00035;
    std::uint32_t episodes = 0;
    bool deepDelivery = false;
};

std::vector<Peer*>
peerById(PeerGroup const& peers)
{
    std::vector<Peer*> out(peers.size(), nullptr);
    for (auto* p : peers)
        out[static_cast<std::uint32_t>(p->id)] = p;
    return out;
}

void
trustAll(PeerGroup& peers)
{
    for (auto* p : peers)
    {
        for (auto* q : peers)
            p->trust(*q);
    }
}

std::string
sideName(Ledger const& ledger, TxSet::ID const& yesId)
{
    return TxSet::calcID(ledger.txs()) == yesId ? "Y" : "N";
}

std::string
branchName(Ledger const& ledger, TxSet::ID const& yesId)
{
    auto const closeSec = duration_cast<seconds>(ledger.closeTime().time_since_epoch()).count();
    std::stringstream ss;
    ss << sideName(ledger, yesId) << "/CT=";
    if (ledger.closeAgree())
        ss << closeSec;
    else
        ss << "no-consensus(effective=" << closeSec << ")";
    return ss.str();
}

std::string
maskString(std::uint8_t mask)
{
    std::stringstream ss;
    ss << "{";
    bool first = true;
    for (std::uint32_t i = 0; i < kNodes; ++i)
    {
        if ((mask & (1u << i)) == 0)
            continue;
        if (!first)
            ss << ",";
        ss << i;
        first = false;
    }
    ss << "}";
    return ss.str();
}

Candidate
randomCandidate(std::mt19937_64& rng)
{
    Candidate c;

    std::array<std::uint32_t, kNodes> ids{};
    std::iota(ids.begin(), ids.end(), 0);
    std::ranges::shuffle(ids, rng);

    std::uniform_int_distribution<std::uint32_t> yesCountDist(3, 4);
    std::uint32_t const yesCount = yesCountDist(rng);
    for (std::uint32_t i = 0; i < yesCount; ++i)
        c.initialYes[ids[i]] = true;

    std::ranges::shuffle(ids, rng);
    std::uniform_int_distribution<std::uint32_t> highCloseCountDist(2, 5);
    std::uint32_t const highCloseCount = highCloseCountDist(rng);
    c.initialCloseTimeSec.fill(kCloseLowSec);
    for (std::uint32_t i = 0; i < highCloseCount; ++i)
        c.initialCloseTimeSec[ids[i]] = kCloseHighSec;

    std::uniform_int_distribution<int> establishJitter(0, 1200);
    std::uniform_int_distribution<std::size_t> prevChoice(0, kPrevChoicesMs.size() - 1);
    std::uniform_int_distribution<int> tickJitter(-250, 250);

    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        c.establishUs[node] = establishJitter(rng) * 1000LL;
        c.prevRoundMs[node] = kPrevChoicesMs[prevChoice(rng)];
        for (std::uint32_t tick = 0; tick < kTicksPerNode; ++tick)
        {
            c.tickUs[node][tick] =
                (static_cast<std::int64_t>(kBaseTicksMs[tick]) + tickJitter(rng)) * 1000LL;
        }
    }

    std::bernoulli_distribution deliverBit(0.68);
    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        for (std::uint32_t tick = 0; tick < kTicksPerNode; ++tick)
        {
            std::uint8_t mask = 0;
            for (std::uint32_t sender = 0; sender < kNodes; ++sender)
            {
                if (sender == node || deliverBit(rng))
                    mask |= static_cast<std::uint8_t>(1u << sender);
            }
            c.deliveryMask[node][tick] = mask;
        }
    }

    return c;
}

double
clampProb(double p)
{
    return std::clamp(p, 0.05, 0.95);
}

double
clampMs(double value, double lo, double hi)
{
    return std::clamp(value, lo, hi);
}

double
sigmoid(double x)
{
    x = std::clamp(x, -30.0, 30.0);
    return 1.0 / (1.0 + std::exp(-x));
}

double
dot(std::array<double, kRlFeatures> const& a, std::array<double, kRlFeatures> const& b)
{
    double out = 0.0;
    for (std::size_t i = 0; i < kRlFeatures; ++i)
        out += a[i] * b[i];
    return out;
}

void
initializeDeepDelivery(RlPolicy& policy, std::mt19937_64& rng)
{
    std::normal_distribution<double> init{0.0, 0.08};
    for (auto& row : policy.deliveryW1)
    {
        for (double& w : row)
            w = init(rng);
    }
    for (double& w : policy.deliveryW2)
        w = init(rng);
}

std::pair<double, std::array<double, kRlHidden>>
deepDeliveryForward(
    RlPolicy const& policy,
    std::array<double, kRlFeatures> const& features)
{
    std::array<double, kRlHidden> hidden{};
    double logit = policy.deliveryB2;
    for (std::size_t h = 0; h < kRlHidden; ++h)
    {
        double z = policy.deliveryB1[h];
        for (std::size_t i = 0; i < kRlFeatures; ++i)
            z += policy.deliveryW1[h][i] * features[i];
        hidden[h] = std::tanh(z);
        logit += policy.deliveryW2[h] * hidden[h];
    }
    return {std::clamp(sigmoid(logit), 0.02, 0.98), hidden};
}

std::array<double, kRlFeatures>
initialFeatures(std::uint32_t node, bool closeTimeDecision)
{
    double const nodeNorm = static_cast<double>(node) / static_cast<double>(kNodes - 1);
    return {
        1.0,
        closeTimeDecision ? 1.0 : -1.0,
        nodeNorm,
        1.0 - nodeNorm,
        (node % 2 == 0) ? 1.0 : -1.0,
        node < (kNodes / 2) ? 1.0 : -1.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0};
}

std::array<double, kRlFeatures>
deliveryFeatures(
    Candidate const& candidate,
    Proposal const& proposal,
    TxSet::ID const& yesId,
    std::uint32_t receiver,
    std::uint32_t sender,
    std::uint32_t tick)
{
    auto const proposalCloseSec =
        duration_cast<seconds>(proposal.closeTime().time_since_epoch()).count();
    bool const receiverYes = candidate.initialYes[receiver];
    bool const senderYes = candidate.initialYes[sender];
    bool const receiverHighClose = candidate.initialCloseTimeSec[receiver] == kCloseHighSec;
    bool const senderHighClose = candidate.initialCloseTimeSec[sender] == kCloseHighSec;
    bool const proposalYes = proposal.position() == yesId;
    bool const proposalHighClose = proposalCloseSec >= kCloseHighSec;

    return {
        1.0,
        receiverYes ? 1.0 : -1.0,
        senderYes ? 1.0 : -1.0,
        receiverYes == senderYes ? 1.0 : -1.0,
        receiverHighClose ? 1.0 : -1.0,
        senderHighClose ? 1.0 : -1.0,
        receiverHighClose == senderHighClose ? 1.0 : -1.0,
        static_cast<double>(tick) / static_cast<double>(kTicksPerNode - 1),
        proposalYes ? 1.0 : -1.0,
        proposalHighClose ? 1.0 : -1.0,
        std::min<double>(proposal.proposeSeq(), 5.0) / 5.0,
        (static_cast<double>(sender) - static_cast<double>(receiver)) /
            static_cast<double>(kNodes - 1)};
}

bool
sampleRlAction(
    std::array<double, kRlFeatures> const& weights,
    std::array<double, kRlFeatures> const& features,
    std::mt19937_64& rng,
    std::vector<RlDecision>& trajectory,
    bool initialAction)
{
    double const p = std::clamp(sigmoid(dot(weights, features)), 0.02, 0.98);
    bool const action = std::bernoulli_distribution{p}(rng);
    trajectory.push_back(RlDecision{features, action, p, initialAction});
    return action;
}

bool
sampleRlDelivery(
    RlPolicy const& policy,
    std::array<double, kRlFeatures> const& features,
    std::mt19937_64& rng,
    std::vector<RlDecision>& trajectory)
{
    if (policy.deepDelivery)
    {
        auto const [p, _] = deepDeliveryForward(policy, features);
        bool const action = std::bernoulli_distribution{p}(rng);
        trajectory.push_back(RlDecision{features, action, p, false});
        return action;
    }
    return sampleRlAction(policy.deliveryWeights, features, rng, trajectory, false);
}

Candidate
reinforceCandidate(
    RlPolicy const& policy,
    std::mt19937_64& rng,
    std::vector<RlDecision>& trajectory)
{
    Candidate c = randomCandidate(rng);
    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        c.initialYes[node] =
            sampleRlAction(policy.initialWeights, initialFeatures(node, false), rng, trajectory, true);
        bool const highClose =
            sampleRlAction(policy.initialWeights, initialFeatures(node, true), rng, trajectory, true);
        c.initialCloseTimeSec[node] = highClose ? kCloseHighSec : kCloseLowSec;
    }
    return c;
}

void
updateRlPolicy(RlPolicy& policy, std::vector<RlDecision> const& trajectory, double reward)
{
    ++policy.episodes;
    if (policy.episodes == 1)
        policy.baseline = reward;
    else
        policy.baseline = 0.95 * policy.baseline + 0.05 * reward;

    double const advantage = std::clamp(reward - policy.baseline, -6000.0, 6000.0);
    double const scale = trajectory.empty() ? 0.0 : advantage / trajectory.size();

    std::array<std::array<double, kRlFeatures>, kRlHidden> gradW1{};
    std::array<double, kRlHidden> gradB1{};
    std::array<double, kRlHidden> gradW2{};
    double gradB2 = 0.0;

    for (RlDecision const& decision : trajectory)
    {
        double const grad = (decision.action ? 1.0 : 0.0) - decision.probability;
        if (policy.deepDelivery && !decision.initialAction)
        {
            auto const [_, hidden] = deepDeliveryForward(policy, decision.features);
            double const g = scale * grad;
            gradB2 += g;
            for (std::size_t h = 0; h < kRlHidden; ++h)
            {
                gradW2[h] += g * hidden[h];
                double const hiddenGrad =
                    g * policy.deliveryW2[h] * (1.0 - hidden[h] * hidden[h]);
                gradB1[h] += hiddenGrad;
                for (std::size_t i = 0; i < kRlFeatures; ++i)
                    gradW1[h][i] += hiddenGrad * decision.features[i];
            }
            continue;
        }

        auto& weights = decision.initialAction ? policy.initialWeights : policy.deliveryWeights;
        for (std::size_t i = 0; i < kRlFeatures; ++i)
        {
            weights[i] += policy.learningRate * scale * grad * decision.features[i];
            weights[i] = std::clamp(weights[i], -8.0, 8.0);
        }
    }

    if (policy.deepDelivery)
    {
        for (std::size_t h = 0; h < kRlHidden; ++h)
        {
            for (std::size_t i = 0; i < kRlFeatures; ++i)
            {
                policy.deliveryW1[h][i] += policy.learningRate * gradW1[h][i];
                policy.deliveryW1[h][i] = std::clamp(policy.deliveryW1[h][i], -8.0, 8.0);
            }
            policy.deliveryB1[h] += policy.learningRate * gradB1[h];
            policy.deliveryB1[h] = std::clamp(policy.deliveryB1[h], -8.0, 8.0);
            policy.deliveryW2[h] += policy.learningRate * gradW2[h];
            policy.deliveryW2[h] = std::clamp(policy.deliveryW2[h], -8.0, 8.0);
        }
        policy.deliveryB2 += policy.learningRate * gradB2;
        policy.deliveryB2 = std::clamp(policy.deliveryB2, -8.0, 8.0);
    }
}

Policy
initialPolicy()
{
    Policy p;
    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        p.initialYesProb[node] = 0.5;
        p.closeTimeHighProb[node] = 0.5;
        p.establishMeanMs[node] = 600.0;
        p.establishStdMs[node] = 360.0;
        for (std::size_t i = 0; i < kPrevChoicesMs.size(); ++i)
            p.prevRoundProb[node][i] = 1.0 / kPrevChoicesMs.size();

        for (std::uint32_t tick = 0; tick < kTicksPerNode; ++tick)
        {
            p.tickMeanMs[node][tick] = kBaseTicksMs[tick];
            p.tickStdMs[node][tick] = 180.0;
            for (std::uint32_t sender = 0; sender < kNodes; ++sender)
                p.deliveryProb[node][tick][sender] = sender == node ? 1.0 : 0.68;
        }
    }
    return p;
}

std::size_t
sampleCategorical(std::array<double, kPrevChoicesMs.size()> const& probs, std::mt19937_64& rng)
{
    std::discrete_distribution<std::size_t> dist(probs.begin(), probs.end());
    return dist(rng);
}

Candidate
sampleCandidate(Policy const& p, std::mt19937_64& rng)
{
    Candidate c;
    for (std::uint32_t node = 0; node < kNodes; ++node)
        c.initialYes[node] = std::bernoulli_distribution{p.initialYesProb[node]}(rng);

    std::uint32_t yesCount =
        static_cast<std::uint32_t>(std::count(c.initialYes.begin(), c.initialYes.end(), true));
    std::array<std::uint32_t, kNodes> ids{};
    std::iota(ids.begin(), ids.end(), 0);
    std::ranges::shuffle(ids, rng);
    if (yesCount < 3)
    {
        for (std::uint32_t id : ids)
        {
            if (!c.initialYes[id])
            {
                c.initialYes[id] = true;
                if (++yesCount == 3)
                    break;
            }
        }
    }
    else if (yesCount > 4)
    {
        for (std::uint32_t id : ids)
        {
            if (c.initialYes[id])
            {
                c.initialYes[id] = false;
                if (--yesCount == 4)
                    break;
            }
        }
    }

    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        bool const highClose = std::bernoulli_distribution{p.closeTimeHighProb[node]}(rng);
        c.initialCloseTimeSec[node] = highClose ? kCloseHighSec : kCloseLowSec;
    }

    std::uint32_t highCloseCount = static_cast<std::uint32_t>(std::count(
        c.initialCloseTimeSec.begin(), c.initialCloseTimeSec.end(), kCloseHighSec));
    std::ranges::shuffle(ids, rng);
    if (highCloseCount < 2)
    {
        for (std::uint32_t id : ids)
        {
            if (c.initialCloseTimeSec[id] != kCloseHighSec)
            {
                c.initialCloseTimeSec[id] = kCloseHighSec;
                if (++highCloseCount == 2)
                    break;
            }
        }
    }
    else if (highCloseCount > 5)
    {
        for (std::uint32_t id : ids)
        {
            if (c.initialCloseTimeSec[id] == kCloseHighSec)
            {
                c.initialCloseTimeSec[id] = kCloseLowSec;
                if (--highCloseCount == 5)
                    break;
            }
        }
    }

    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        std::normal_distribution<double> establishDist{
            p.establishMeanMs[node], std::max(1.0, p.establishStdMs[node])};
        c.establishUs[node] =
            static_cast<std::int64_t>(std::llround(clampMs(establishDist(rng), 0.0, 1200.0))) *
            1000LL;

        c.prevRoundMs[node] = kPrevChoicesMs[sampleCategorical(p.prevRoundProb[node], rng)];

        for (std::uint32_t tick = 0; tick < kTicksPerNode; ++tick)
        {
            std::normal_distribution<double> tickDist{
                p.tickMeanMs[node][tick], std::max(1.0, p.tickStdMs[node][tick])};
            double const lower = kBaseTicksMs[tick] - 300.0;
            double const upper = kBaseTicksMs[tick] + 300.0;
            c.tickUs[node][tick] =
                static_cast<std::int64_t>(std::llround(clampMs(tickDist(rng), lower, upper))) *
                1000LL;

            std::uint8_t mask = 0;
            for (std::uint32_t sender = 0; sender < kNodes; ++sender)
            {
                if (sender == node ||
                    std::bernoulli_distribution{p.deliveryProb[node][tick][sender]}(rng))
                    mask |= static_cast<std::uint8_t>(1u << sender);
            }
            c.deliveryMask[node][tick] = mask;
        }
    }

    return c;
}

void
updatePolicy(
    Policy& p,
    std::vector<RunResult> const& ranked,
    std::size_t eliteCount)
{
    double constexpr keep = 0.65;
    double constexpr learn = 1.0 - keep;
    eliteCount = std::min(eliteCount, ranked.size());
    if (eliteCount == 0)
        return;

    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        double yesMean = 0.0;
        double highCloseMean = 0.0;
        double establishMean = 0.0;
        std::array<double, kPrevChoicesMs.size()> prevCounts{};
        std::array<double, kTicksPerNode> tickMeans{};
        std::array<std::array<double, kNodes>, kTicksPerNode> deliveryMeans{};

        for (std::size_t i = 0; i < eliteCount; ++i)
        {
            Candidate const& c = ranked[i].candidate;
            yesMean += c.initialYes[node] ? 1.0 : 0.0;
            highCloseMean += c.initialCloseTimeSec[node] == kCloseHighSec ? 1.0 : 0.0;
            establishMean += static_cast<double>(c.establishUs[node]) / 1000.0;
            for (std::size_t choice = 0; choice < kPrevChoicesMs.size(); ++choice)
            {
                if (c.prevRoundMs[node] == static_cast<std::uint32_t>(kPrevChoicesMs[choice]))
                    prevCounts[choice] += 1.0;
            }
            for (std::uint32_t tick = 0; tick < kTicksPerNode; ++tick)
            {
                tickMeans[tick] += static_cast<double>(c.tickUs[node][tick]) / 1000.0;
                for (std::uint32_t sender = 0; sender < kNodes; ++sender)
                {
                    if ((c.deliveryMask[node][tick] & (1u << sender)) != 0)
                        deliveryMeans[tick][sender] += 1.0;
                }
            }
        }

        yesMean /= eliteCount;
        highCloseMean /= eliteCount;
        establishMean /= eliteCount;
        p.initialYesProb[node] = clampProb(keep * p.initialYesProb[node] + learn * yesMean);
        p.closeTimeHighProb[node] =
            clampProb(keep * p.closeTimeHighProb[node] + learn * highCloseMean);
        p.establishMeanMs[node] =
            clampMs(keep * p.establishMeanMs[node] + learn * establishMean, 0.0, 1200.0);

        double establishVar = 0.0;
        for (std::size_t i = 0; i < eliteCount; ++i)
        {
            double const v = static_cast<double>(ranked[i].candidate.establishUs[node]) / 1000.0;
            establishVar += (v - establishMean) * (v - establishMean);
        }
        establishVar /= eliteCount;
        p.establishStdMs[node] =
            clampMs(keep * p.establishStdMs[node] + learn * std::sqrt(establishVar), 25.0, 420.0);

        for (std::size_t choice = 0; choice < kPrevChoicesMs.size(); ++choice)
        {
            double const eliteProb = prevCounts[choice] / eliteCount;
            p.prevRoundProb[node][choice] =
                clampProb(keep * p.prevRoundProb[node][choice] + learn * eliteProb);
        }

        double norm = 0.0;
        for (double prob : p.prevRoundProb[node])
            norm += prob;
        for (double& prob : p.prevRoundProb[node])
            prob /= norm;

        for (std::uint32_t tick = 0; tick < kTicksPerNode; ++tick)
        {
            tickMeans[tick] /= eliteCount;
            p.tickMeanMs[node][tick] = clampMs(
                keep * p.tickMeanMs[node][tick] + learn * tickMeans[tick],
                kBaseTicksMs[tick] - 300.0,
                kBaseTicksMs[tick] + 300.0);

            double tickVar = 0.0;
            for (std::size_t i = 0; i < eliteCount; ++i)
            {
                double const v = static_cast<double>(ranked[i].candidate.tickUs[node][tick]) / 1000.0;
                tickVar += (v - tickMeans[tick]) * (v - tickMeans[tick]);
            }
            tickVar /= eliteCount;
            p.tickStdMs[node][tick] =
                clampMs(keep * p.tickStdMs[node][tick] + learn * std::sqrt(tickVar), 20.0, 220.0);

            for (std::uint32_t sender = 0; sender < kNodes; ++sender)
            {
                double const eliteProb = deliveryMeans[tick][sender] / eliteCount;
                p.deliveryProb[node][tick][sender] =
                    sender == node
                    ? 1.0
                    : clampProb(keep * p.deliveryProb[node][tick][sender] + learn * eliteProb);
            }
        }
    }
}

Candidate
bug5StyleSeed(std::mt19937_64& rng)
{
    Candidate c;
    c.initialYes = {true, true, true, false, false, true, false};
    c.initialCloseTimeSec.fill(kCloseLowSec);

    std::uniform_int_distribution<int> smallJitter(-40, 40);
    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        c.establishUs[node] = (800 + smallJitter(rng)) * 1000LL;
        c.prevRoundMs[node] = 5000;
        c.tickUs[node] = {
            (3000 + smallJitter(rng)) * 1000LL,
            (4000 + smallJitter(rng)) * 1000LL,
            (19000 + smallJitter(rng)) * 1000LL,
            (20000 + smallJitter(rng)) * 1000LL};
    }

    // These masks describe who each node is allowed to hear before its kth
    // timer tick. Proposals are still the proposals emitted by Consensus.
    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        for (std::uint32_t tick = 0; tick < kTicksPerNode; ++tick)
            c.deliveryMask[node][tick] = 0x7f;
    }

    c.deliveryMask[5][0] = 0b1111111;
    c.deliveryMask[2][0] = 0b1111111;
    c.deliveryMask[0][0] = 0b1111111;
    c.deliveryMask[6][0] = 0b0111111;
    c.deliveryMask[3][0] = 0b1111111;
    c.deliveryMask[4][0] = 0b1111111;
    c.deliveryMask[1][0] = 0b1111111;

    c.deliveryMask[5][1] = 0b1111111;
    c.deliveryMask[2][1] = 0b1111111;
    c.deliveryMask[0][1] = 0b1111111;
    c.deliveryMask[1][1] = 0b1111111;

    return c;
}

Candidate
makeCandidate(std::mt19937_64& rng, std::uint32_t attempt)
{
    // Interleave broad random candidates with candidates near the known
    // txset-failure geometry. The latter is still a search over timings and
    // deliveries; it does not prescribe honest proposal contents.
    if (attempt % 5 == 0)
        return bug5StyleSeed(rng);
    return randomCandidate(rng);
}

RunResult
runCandidate(
    Candidate const& candidate,
    RlPolicy const* rlPolicy = nullptr,
    std::mt19937_64* rlRng = nullptr,
    std::vector<RlDecision>* rlTrajectory = nullptr)
{
    RunResult result;
    result.candidate = candidate;

    Sim sim;
    SearchCollector collector;
    sim.collectors.add(collector);

    auto peers = sim.createGroup(kNodes);
    trustAll(peers);
    auto byId = peerById(peers);
    auto peer = [&](std::uint32_t id) -> Peer* { return byId.at(id); };

    TxSetType noTxs;
    TxSetType yesTxs{Tx{1}};
    TxSet noSet{noTxs};
    TxSet yesSet{yesTxs};

    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        Peer* p = peer(node);
        p->targetLedgers = 0;
        p->fakeSetPreviousRound(milliseconds{candidate.prevRoundMs[node]}, kNodes);
        p->handle(noSet);
        p->handle(yesSet);
        if (candidate.initialYes[node])
            p->openTxs.insert(Tx{1});
    }

    std::set<std::tuple<
        std::uint32_t,
        std::uint32_t,
        std::uint32_t,
        TxSet::ID,
        xrpl::NetClock::time_point>> delivered;

    auto deliverLatestProposalsTo =
        [&](std::uint32_t receiver, std::uint32_t tick, std::uint8_t mask) {
        for (std::uint32_t sender = 0; sender < kNodes; ++sender)
        {
            if (sender == receiver)
                continue;

            if (!rlPolicy && (mask & (1u << sender)) == 0)
                continue;

            auto it = collector.proposalShares.find(PeerID{sender});
            if (it == collector.proposalShares.end() || it->second.empty())
            {
                if (!rlPolicy)
                {
                    ++result.requestedProposalDeliveries;
                    ++result.missedProposalDeliveries;
                }
                continue;
            }

            Proposal const& proposal = it->second.back().val;
            auto key = std::make_tuple(
                receiver, sender, proposal.proposeSeq(), proposal.position(), proposal.closeTime());
            if (delivered.find(key) != delivered.end())
                continue;

            ++result.requestedProposalDeliveries;
            if (rlPolicy)
            {
                if (!rlRng || !rlTrajectory)
                    throw std::runtime_error("RL delivery requires rng and trajectory");
                auto const features =
                    deliveryFeatures(candidate, proposal, yesSet.id(), receiver, sender, tick);
                if (!sampleRlDelivery(*rlPolicy, features, *rlRng, *rlTrajectory))
                    continue;
            }

            delivered.insert(key);
            peer(receiver)->handle(proposal);
            ++result.deliveredProposals;
        }
    };

    auto deliverValidationsTo = [&](std::uint32_t receiver) {
        auto acceptedLedgers = collector.acceptedLedgers();
        Peer* p = peer(receiver);
        for (auto const& [sender, shares] : collector.validationShares)
        {
            if (sender == PeerID{receiver})
                continue;
            for (auto const& share : shares)
            {
                auto ledger = acceptedLedgers.find(share.val.ledgerID());
                if (ledger != acceptedLedgers.end())
                    p->ledgers[ledger->first] = ledger->second;
                p->handle(share.val);
            }
        }
    };

    auto const start = sim.scheduler.now();
    auto scheduleAt = [&](std::int64_t atUs, auto&& f) {
        sim.scheduler.at(start + microseconds{atUs}, std::forward<decltype(f)>(f));
    };

    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        scheduleAt(0, [&, node] { peer(node)->startRound(); });
        scheduleAt(candidate.establishUs[node], [&, node] {
            peer(node)->fakeCloseLedger(
                xrpl::NetClock::time_point{seconds{candidate.initialCloseTimeSec[node]}});
        });

        for (std::uint32_t tick = 0; tick < kTicksPerNode; ++tick)
        {
            scheduleAt(candidate.tickUs[node][tick], [&, node, tick] {
                deliverLatestProposalsTo(node, tick, candidate.deliveryMask[node][tick]);
                deliverValidationsTo(node);
                peer(node)->timerEntryOnce();
            });
        }
    }

    sim.scheduler.step();

    auto const acceptedLedgers = collector.acceptedLedgers();
    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        auto it = collector.accepts.find(PeerID{node});
        if (it == collector.accepts.end() || it->second.empty())
            continue;
        ++result.acceptedCount;
        Ledger const& ledger = it->second.front().ledger;
        result.acceptedSides[branchName(ledger, yesSet.id())].push_back(node);
    }
    for (auto const& [_, nodes] : result.acceptedSides)
    {
        result.maxAcceptedVotes =
            std::max<std::uint32_t>(result.maxAcceptedVotes, nodes.size());
    }

    std::map<Ledger::ID, std::vector<std::uint32_t>> validationsByLedger;
    for (auto const& [sender, shares] : collector.validationShares)
    {
        if (shares.empty())
            continue;
        ++result.validationCount;
        Ledger::ID const ledgerId = shares.front().val.ledgerID();
        validationsByLedger[ledgerId].push_back(static_cast<std::uint32_t>(sender));
        auto ledger = acceptedLedgers.find(ledgerId);
        std::string side =
            ledger == acceptedLedgers.end() ? "unknown" : branchName(ledger->second, yesSet.id());
        result.validationSides[side].push_back(static_cast<std::uint32_t>(sender));
    }

    for (auto const& [_, nodes] : validationsByLedger)
    {
        result.maxValidationVotes =
            std::max<std::uint32_t>(result.maxValidationVotes, nodes.size());
    }

    auto branchBonus = [](std::map<std::string, std::vector<std::uint32_t>> const& branches) {
        if (branches.empty())
            return 0.0;
        double total = 0;
        for (auto const& [_, nodes] : branches)
            total += nodes.size();
        double entropy = 0;
        for (auto const& [_, nodes] : branches)
        {
            double const p = nodes.size() / total;
            entropy -= p * std::log2(p);
        }
        return entropy * 100.0;
    };

    auto hasSplit = [](std::map<std::string, std::vector<std::uint32_t>> const& branches) {
        std::uint32_t nonEmpty = 0;
        for (auto const& [_, nodes] : branches)
        {
            if (!nodes.empty())
                ++nonEmpty;
        }
        return nonEmpty >= 2;
    };

    bool const splitObserved = hasSplit(result.acceptedSides) || hasSplit(result.validationSides);
    result.violation = result.acceptedCount == kNodes &&
        result.validationCount >= 4 &&
        result.maxAcceptedVotes > 0 &&
        result.maxAcceptedVotes < kQuorum &&
        result.maxValidationVotes > 0 &&
        result.maxValidationVotes < kQuorum &&
        splitObserved;

    result.score = result.acceptedCount * 250.0 + result.validationCount * 80.0;
    if (result.acceptedCount == kNodes)
    {
        result.score +=
            (kQuorum - std::min(kQuorum, result.maxAcceptedVotes)) * 180.0 +
            branchBonus(result.acceptedSides) * 2.0;
    }
    else
    {
        result.score -= (kNodes - result.acceptedCount) * 300.0;
    }

    if (result.validationCount >= 4)
    {
        result.score +=
            (kQuorum - std::min(kQuorum, result.maxValidationVotes)) * 100.0 +
            branchBonus(result.validationSides);
    }
    else
    {
        result.score -= (4 - result.validationCount) * 160.0;
    }

    if (result.validationCount == kNodes && result.maxValidationVotes >= kQuorum)
        result.score -= 500.0;
    if (!splitObserved)
        result.score -= 250.0;
    result.score -= result.missedProposalDeliveries * 2.0;
    if (result.violation)
        result.score += 5000.0;

    return result;
}

void
printCandidate(Candidate const& c)
{
    std::cout << "initial_yes={";
    bool first = true;
    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        if (!c.initialYes[node])
            continue;
        if (!first)
            std::cout << ",";
        std::cout << node;
        first = false;
    }
    std::cout << "}\n";

    std::cout << "initial_close_time_sec={";
    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        if (node)
            std::cout << ",";
        std::cout << node << ":" << c.initialCloseTimeSec[node];
    }
    std::cout << "}\n";

    for (std::uint32_t node = 0; node < kNodes; ++node)
    {
        std::cout << "node " << node << ": establish_us=" << c.establishUs[node]
                  << " prev_round_ms=" << c.prevRoundMs[node] << " ticks_us=[";
        for (std::uint32_t tick = 0; tick < kTicksPerNode; ++tick)
        {
            if (tick)
                std::cout << ",";
            std::cout << c.tickUs[node][tick];
        }
        std::cout << "] masks=[";
        for (std::uint32_t tick = 0; tick < kTicksPerNode; ++tick)
        {
            if (tick)
                std::cout << ",";
            std::cout << maskString(c.deliveryMask[node][tick]);
        }
        std::cout << "]\n";
    }
}

void
printBranches(std::string const& label, std::map<std::string, std::vector<std::uint32_t>> const& branches)
{
    std::cout << label << ":";
    for (auto const& [side, nodes] : branches)
    {
        std::cout << " " << side << "={";
        for (std::size_t i = 0; i < nodes.size(); ++i)
        {
            if (i)
                std::cout << ",";
            std::cout << nodes[i];
        }
        std::cout << "}";
    }
    std::cout << "\n";
}

void
printFound(RunResult const& result, std::uint32_t attempt, std::uint64_t seed)
{
    std::cout << "\n*** Found candidate liveness violation schedule ***\n";
    std::cout << "attempt=" << attempt << " seed=" << seed << "\n";
    std::cout << "accepted=" << result.acceptedCount
              << " validations=" << result.validationCount
              << " max_accepted_votes=" << result.maxAcceptedVotes
              << " max_validation_votes=" << result.maxValidationVotes
              << " quorum=" << kQuorum << "\n";
    std::cout << "proposal_deliveries: requested=" << result.requestedProposalDeliveries
              << " delivered=" << result.deliveredProposals
              << " missed_not_yet_emitted=" << result.missedProposalDeliveries << "\n";
    printBranches("accepted", result.acceptedSides);
    printBranches("validation", result.validationSides);
    printCandidate(result.candidate);
}

void
printBest(std::string const& prefix, RunResult const& result, std::uint32_t attempt)
{
    std::cout << prefix << " attempt=" << attempt << " score=" << std::fixed
              << std::setprecision(1) << result.score << " accepted="
              << result.acceptedCount << " validations=" << result.validationCount
              << " max_accepted_votes=" << result.maxAcceptedVotes
              << " max_validation_votes=" << result.maxValidationVotes
              << " missed_deliveries=" << result.missedProposalDeliveries << "\n";
    printBranches("accepted", result.acceptedSides);
    printBranches("validation", result.validationSides);
}

int
runRandomSearch(std::uint32_t attempts, std::uint64_t seed)
{
    std::mt19937_64 rng(seed);
    std::optional<RunResult> best;

    for (std::uint32_t attempt = 0; attempt < attempts; ++attempt)
    {
        Candidate const candidate = makeCandidate(rng, attempt);
        RunResult result = runCandidate(candidate);

        if (!best || result.score > best->score)
        {
            best = result;
            printBest("[best]", result, attempt);
        }

        if (result.violation)
        {
            printFound(result, attempt, seed);
            return 0;
        }
    }

    if (best)
    {
        std::cout << "\nNo violation found in " << attempts << " attempts. Best candidate:\n";
        std::cout << "score=" << std::fixed << std::setprecision(1) << best->score
                  << " accepted=" << best->acceptedCount
                  << " validations=" << best->validationCount
                  << " max_accepted_votes=" << best->maxAcceptedVotes
                  << " max_validation_votes=" << best->maxValidationVotes
                  << " missed_deliveries=" << best->missedProposalDeliveries << "\n";
        printBranches("accepted", best->acceptedSides);
        printBranches("validation", best->validationSides);
        printCandidate(best->candidate);
    }
    return 1;
}

int
runPolicySearch(std::uint32_t attempts, std::uint64_t seed)
{
    std::mt19937_64 rng(seed);
    Policy policy = initialPolicy();
    std::optional<RunResult> best;

    std::uint32_t evaluations = 0;
    std::uint32_t generation = 0;
    std::uint32_t constexpr population = 128;

    while (evaluations < attempts)
    {
        std::uint32_t const batch =
            std::min<std::uint32_t>(population, attempts - evaluations);
        std::vector<RunResult> ranked;
        ranked.reserve(batch);

        for (std::uint32_t i = 0; i < batch; ++i)
        {
            Candidate const candidate = sampleCandidate(policy, rng);
            RunResult result = runCandidate(candidate);
            std::uint32_t const attempt = evaluations++;

            if (!best || result.score > best->score)
            {
                best = result;
                printBest("[policy-best]", result, attempt);
            }

            if (result.violation)
            {
                printFound(result, attempt, seed);
                return 0;
            }

            ranked.push_back(std::move(result));
        }

        std::ranges::sort(
            ranked, [](RunResult const& a, RunResult const& b) { return a.score > b.score; });
        std::size_t const eliteCount = std::max<std::size_t>(8, ranked.size() / 5);
        updatePolicy(policy, ranked, eliteCount);

        if (!ranked.empty())
        {
            std::cout << "[policy-gen] generation=" << generation
                      << " evaluations=" << evaluations
                      << " elite_best=" << std::fixed << std::setprecision(1)
                      << ranked.front().score << "\n";
        }
        ++generation;
    }

    if (best)
    {
        std::cout << "\nNo violation found in " << attempts
                  << " policy-search evaluations. Best candidate:\n";
        std::cout << "score=" << std::fixed << std::setprecision(1) << best->score
                  << " accepted=" << best->acceptedCount
                  << " validations=" << best->validationCount
                  << " max_accepted_votes=" << best->maxAcceptedVotes
                  << " max_validation_votes=" << best->maxValidationVotes
                  << " missed_deliveries=" << best->missedProposalDeliveries << "\n";
        printBranches("accepted", best->acceptedSides);
        printBranches("validation", best->validationSides);
        printCandidate(best->candidate);
    }
    return 1;
}

int
runReinforceSearch(std::uint32_t attempts, std::uint64_t seed, bool learnInitial, bool deepDelivery)
{
    std::mt19937_64 rng(seed);
    RlPolicy policy;
    policy.deepDelivery = deepDelivery;
    if (policy.deepDelivery)
        initializeDeepDelivery(policy, rng);
    std::optional<RunResult> best;

    for (std::uint32_t episode = 0; episode < attempts; ++episode)
    {
        std::vector<RlDecision> trajectory;
        Candidate const candidate =
            learnInitial ? reinforceCandidate(policy, rng, trajectory) : randomCandidate(rng);
        RunResult result = runCandidate(candidate, &policy, &rng, &trajectory);

        if (!best || result.score > best->score)
        {
            best = result;
            printBest("[rl-best]", result, episode);
            std::string const label =
                deepDelivery ? (learnInitial ? "[deep-rl-full]" : "[deep-rl]") :
                (learnInitial ? "[rl-full]" : "[rl]");
            std::cout << label << " episode=" << episode
                      << " actions=" << trajectory.size()
                      << " delivered=" << result.deliveredProposals
                      << " baseline=" << std::fixed << std::setprecision(1)
                      << policy.baseline << "\n";
        }

        if (result.violation)
        {
            printFound(result, episode, seed);
            return 0;
        }

        updateRlPolicy(policy, trajectory, result.score);

        if ((episode + 1) % 256 == 0)
        {
            std::cout << "[rl-train] episodes=" << (episode + 1)
                      << " baseline=" << std::fixed << std::setprecision(1)
                      << policy.baseline;
            if (best)
            {
                std::cout << " best_score=" << best->score
                          << " best_max_accepted=" << best->maxAcceptedVotes
                          << " best_max_validation=" << best->maxValidationVotes;
            }
            std::cout << "\n";
        }
    }

    if (best)
    {
        std::cout << "\nNo violation found in " << attempts
                  << " RL episodes. Best candidate:\n";
        std::cout << "score=" << std::fixed << std::setprecision(1) << best->score
                  << " accepted=" << best->acceptedCount
                  << " validations=" << best->validationCount
                  << " max_accepted_votes=" << best->maxAcceptedVotes
                  << " max_validation_votes=" << best->maxValidationVotes
                  << " missed_deliveries=" << best->missedProposalDeliveries << "\n";
        printBranches("accepted", best->acceptedSides);
        printBranches("validation", best->validationSides);
        printCandidate(best->candidate);
    }
    return 1;
}

}  // namespace

int
main(int argc, char** argv)
try
{
    std::uint32_t attempts = 1000;
    std::uint64_t seed = 1;
    if (argc > 1)
        attempts = static_cast<std::uint32_t>(std::stoul(argv[1]));
    if (argc > 2)
        seed = std::stoull(argv[2]);
    std::string mode = argc > 3 ? argv[3] : "random";

    if (mode == "random")
        return runRandomSearch(attempts, seed);
    if (mode == "policy" || mode == "cem")
        return runPolicySearch(attempts, seed);
    if (mode == "rl" || mode == "reinforce" || mode == "pg")
        return runReinforceSearch(attempts, seed, false, false);
    if (mode == "rl-full" || mode == "reinforce-full" || mode == "pg-full")
        return runReinforceSearch(attempts, seed, true, false);
    if (mode == "deep-rl" || mode == "deep" || mode == "drl")
        return runReinforceSearch(attempts, seed, false, true);
    if (mode == "deep-rl-full" || mode == "deep-full" || mode == "drl-full")
        return runReinforceSearch(attempts, seed, true, true);
    throw std::runtime_error("unknown mode: " + mode);
}
catch (std::exception const& e)
{
    std::cerr << "ERROR: " << e.what() << "\n";
    return 2;
}
