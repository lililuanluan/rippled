# XRPL Consensus Algorithm Liveness Violation

## Introduction

XRP Ledger Consensus Protocol (XRPL) uses three phases in each consensus round. The second phase, the establish phase, runs the core consensus algorithm to converge on a transaction set and a close time for the new ledger. We report two failing scenarios: the first fails to agree on a disputed transaction, and the second fails to agree on the close time. In both cases, nodes accept different ledgers at the end of the establish phase. In our longer-running tests, later rounds did not recover from this split, and no new ledger became fully validated. This report focuses on how the first divergent round occurs.

<!-- These scenarios do not rely on weak UNL configurations. We believe they expose a problem in the consensus algorithm and are worth further investigation. -->

We originally observed these scenarios while testing rippled 3.1.0. Because rippled execution is highly dependent on real time, reproducing the same execution directly is non-deterministic. We therefore provide deterministic replays using rippled's native consensus simulation framework ([CSF](https://github.com/XRPLF/rippled/tree/0711a7b493/src/test/csf)). The replay is based on the `develop` commit `0711a7b493` and reproduces both failures, indicating the behavior is still present. The attached file contains the customized CSF code and raw logs. To reproduce both scenarios, unzip it and run:

```bash
cd ripple-csf/replay/
./run.sh
# or
./run.sh txset      # Run only the transaction-set replay
./run.sh closetime  # Run only the close-time replay
```

## Scenario 1: Transaction Set Agreement Failure

### Configuration

We use seven nodes, labeled `p0`, `p1`, ..., `p6`. Every node's UNL contains all seven nodes. <!-- The test harness marks `p3` as Byzantine, but no Byzantine mutation occurs during the divergent round described below. In the replay of this scenario, every delivered proposal, including those from `p3`, must first have been emitted by the unmodified consensus logic. -->

### Description

During the establish phase, a node votes `yes` for a disputed transaction only when its support reaches the current time-dependent threshold. The following snippets are taken from [Consensus.h](https://github.com/XRPLF/rippled/blob/0711a7b493/src/xrpld/consensus/Consensus.h) and [ConsensusParms.h](https://github.com/XRPLF/rippled/blob/0711a7b493/src/xrpld/consensus/ConsensusParms.h).

```cpp
// ConsensusParms.h
    std::map<AvalancheState, AvalancheCutoff> const avalancheCutoffs{
        // {state, {time, percent, nextState}},
        // Initial state: 50% of nodes must vote yes
        {init, {0, 50, mid}},
        // mid-consensus starts after 50% of the previous round time, and
        // requires 65% yes
        {mid, {50, 65, late}},
        // late consensus starts after 85% time, and requires 70% yes
        {late, {85, 70, stuck}},
        // we're stuck after 2x time, requires 95% yes votes
        {stuck, {200, 95, stuck}},
    };
```

```cpp
// Consensus.h
    convergePercent_ = result_->roundTime.read() * 100 /
        std::max<milliseconds>(prevRoundTime_, parms.avMIN_CONSENSUS_TIME);
```

The code above shows that nodes become more reluctant to vote `yes` for a disputed transaction as the establish phase progresses and the threshold rises. The threshold depends on the duration of the previous consensus round and the elapsed time in the current round.

In our scenario, one transaction is disputed when nodes enter the establish phase. Initially, `p0`, `p1`, `p2`, and `p5` vote `yes`, while all other nodes vote `no`.

The following timeline lists the relevant events in chronological order. We use `Y` for a proposed transaction set that includes the disputed transaction and `N` for one that does not. Timestamps use the `mm:ss` format.

By approximately `08:00`, all nodes have entered the establish phase. Nodes `p0`, `p1`, `p2`, and `p5` initially propose `Y`, while `p3`, `p4`, and `p6` propose `N`. The subsequent events are shown below. Here, *keep* means retaining the current `Y`/`N` position, while *flip* means changing it. *weight* is the current `yes` support percentage and *required* is the time-dependent threshold described above.

| time | node | received peer votes before decision | self before | node action | self after | accept/validate |
| --- | --- | --- | --- | --- | --- | --- |
| `08:03` | `5` | `Y:{0,1,2}`, `N:{3,4,6}` | `Y` | keep `Y`, `weight=57`, `required=50` | `Y` |  |
| `08:03` | `2` | `Y:{0,1,5}`, `N:{3,4,6}` | `Y` | keep `Y`, `weight=57`, `required=50` | `Y` |  |
| `08:03` | `0` | `Y:{1,2,5}`, `N:{3,4,6}` | `Y` | keep `Y`, `weight=57`, `required=50` | `Y` |  |
| `08:03` | `6` | `Y:{0,1,2,5}`, `N:{3,4}` | `N` | flip to `Y`, `weight=57`, `required=50` | `Y` |  |
| `08:03` | `3` | `Y:{0,1,2,5,6}`, `N:{4}` | `N` | after receiving `p6`'s flipped `Y`, flip to `Y`, `weight=71`, `required=50` | `Y` | accept `Y`; validate `Y` |
| `08:03` | `4` | `Y:{0,1,2,5,6}`, `N:{3}` | `N` | before receiving `p3`'s flipped `Y`, flip to `Y`, `weight=71`, `required=50` | `Y` | accept `Y`; validate `Y` |
| `08:03` | `1` | `Y:{0,2,5}`, `N:{3,4,6}` | `Y` | based on the initial proposals, keep `Y`, `weight=57`, `required=50` | `Y` |  |
| `08:04` | `5` | `Y:{0,1,2}`, `N:{3,4,6}` | `Y` | based on the initial proposals, **flip to `N`**, `weight=57`, `required=65`; **proposals unchanged but threshold changed** | `N` |  |
| `08:04` | `2` | `Y:{0,1}`, `N:{3,4,5,6}` | `Y` | after receiving `p5`'s flipped `N`, flip to `N`, `weight=42`, `required=65` | `N` |  |
| `08:04` | `0` | `Y:{1}`, `N:{2,3,4,5,6}` | `Y` | after receiving `p2`'s flipped `N`, flip to `N`, `weight=28`, `required=65` | `N` | accept `N`; validate `N` |
| `08:04` | `6` | `Y:{0,1,2,3,4,5}`, `N:{}` | `Y` | consensus reached | `Y` | accept `Y`; validate `Y` |
| `08:04` | `1` | `Y:{3,4,6}`, `N:{0,2,5}` | `Y` | after receiving `p0`, `p2` and `p5`'s flipped `N`, flip to `N`, `weight=57`, `required=65` | `N` |  |
| `08:19` | `1` | `Y:{3,4,6}`, `N:{0,2,5}` | `N` | round timeout; bow out; accept local `N` | `N` | accept `N`; validate `N` |
| `08:20` | `5` | active peers: `Y:{3,4,6}`, `N:{0,2}`; bowed out: `p1` | `N` | no transaction-set consensus; all active peers moved on; accept local `N` | `N` | accept `N` |
| `08:20` | `2` | active peers: `Y:{3,4,6}`, `N:{0,5}`; bowed out: `p1` | `N` | no transaction-set consensus; all active peers moved on; accept local `N` | `N` | accept `N` |

This round ends with a `4:3` split in accepted ledgers and a `3:2` split in validations. Neither branch reaches the required 6-of-7 validation quorum. In our continued execution, later rounds remained on incompatible branches and did not recover a fully validated ledger, causing a liveness violation.

### Steps to Reproduce

To reproduce this behavior, the replay fixes the runtime inputs that are otherwise timing-dependent:

- `roundStartUs[i]`: the time when node `i` enters the consensus round
- `establishUs[i]`: the time when node `i` enters the establish phase
- `prevRoundTimeMs[i]`: the duration of node `i`'s previous consensus round

These values determine the progress of the avalanche thresholds.

- `TimerTick[i][k]`: the time when node `i` calls its timer-tick handler for the `k`-th time
- `ProposalDelivery {atUs, sender, receiver, proposalSeq, positionHash, closeTime}`: a proposal delivery record

The replay registers these events with CSF's scheduler. For each `ProposalDelivery`, it searches the collector for a proposal already emitted by the sender with the expected sequence and position. The replay fails if an honest proposal has not been emitted; it does not force honest nodes to adopt trace positions. At completion, it checks that the final accepted ledgers and validations match the trace.

For example, the used values in this scenario include:
- `roundStartUs[i]`: `roundStartUs[0]` = 26783873, `roundStartUs[1]` = 26026457, ...
- `establishUs[i]`: `establishUs[0]` = 27780975, `establishUs[1]` = 27023890, ...
- `prevRoundTimeMs[i]`: `prevRoundTimeMs[0]` = 2001, `prevRoundTimeMs[1]` = 2001, ...
- `TimerTick[i][k]`: `TimerTick[0][0]` = 29782704, `TimerTick[0][1]` = 30783765, `TimerTick[1][0]` = 29025452, ...
- `ProposalDelivery` = [{atUs: 26885499, sender: 6, receiver: 3, proposalSeq: 0, positionHash: D4190DD6…, closeTime: 833987279}, ...]


## Scenario 2: Close-Time Agreement Failure

### Configuration

This failure uses seven nodes, `p0`, `p1`, ..., `p6`. Nodes `p0` through `p5` fully trust one another, while `p6` trusts all seven nodes. Node `p3` is Byzantine.

Although this scenario uses a different UNL configuration, it satisfies the stated [fault-tolerance bound](https://ripple.com/files/ripple_consensus_whitepaper.pdf) and [UNL-overlap bound](https://arxiv.org/abs/1802.07242). We do not believe the failure is specific to insufficient UNL overlap.

### Description

The following code from [LedgerTiming.h](https://github.com/XRPLF/rippled/blob/0711a7b493/include/xrpl/ledger/LedgerTiming.h) computes the rounded close times used in consensus. A raw close time is rounded to the nearest multiple of the resolution by first adding half of the resolution and then rounding down.

```cpp
template <class Clock, class Duration, class Rep, class Period>
std::chrono::time_point<Clock, Duration>
roundCloseTime(
    std::chrono::time_point<Clock, Duration> closeTime,
    std::chrono::duration<Rep, Period> closeResolution)
{
    using time_point = decltype(closeTime);
    if (closeTime == time_point{})
        return closeTime;

    closeTime += (closeResolution / 2);
    return closeTime - (closeTime.time_since_epoch() % closeResolution);
}
```
Under this procedure, two close times that differ by one second can fall into different buckets. Given `closeResolution=10`:

- roundCloseTime(74) = $(74 + 5) - (74 + 5) \% 10 = 70$
- roundCloseTime(75) = $(75 + 5) - (75 + 5) \% 10 = 80$

For readability, we use `70` and `80` to denote the rounded close times `834276470` and `834276480`, respectively.

The following code from [Consensus.h](https://github.com/XRPLF/rippled/blob/0711a7b493/src/xrpld/consensus/Consensus.h) updates a node's own close-time proposal based on its peer proposals:

```cpp
NetClock::time_point consensusCloseTime = {};
std::map<NetClock::time_point, int> closeTimeVotes;
// Threshold for non-zero vote
int threshVote = participantsNeeded(participants, neededWeight);
for (auto const& [t, v] : closeTimeVotes)
        {
            if (v >= threshVote)
            {
                // A close time has enough votes for us to try to agree
                consensusCloseTime = t;
                threshVote = v;

                if (threshVote >= threshConsensus)
                    haveCloseTimeConsensus_ = true;
            }
        }
if (!ourNewSet &&
        ((consensusCloseTime != asCloseTime(result_->position.closeTime())) ||
         result_->position.isStale(ourCutoff)))
    {
        // close time changed or our position is stale
        ourNewSet.emplace(result_->txns);
    }
```

This code has two implications:

- If no close-time bucket has at least `threshVote` votes, the next proposed close time is `0`.
- If multiple buckets reach the threshold with the same vote count, the selected value is the last one visited. Because `closeTimeVotes` is a `std::map`, this is the largest close time among the tied buckets.

In the following table, `thrV` is the vote threshold for selecting the next proposed close time, and `thrC` is the threshold for close-time consensus.

The honest nodes enter the establish phase with the following rounded close times: `{p1, p2, p5, p6}: 70` and `{p0, p4}: 80`. Byzantine node `p3` sends receiver-specific close-time proposals.

The table describes close-time vote buckets. For example, `accept 70` means reaching consensus on bucket `70`; the ledger's effective close-time field may differ because it must be later than the parent ledger's close time.

| time | node | close-time votes before decision | self before | node action | self after | accept / validation |
| --- | --- | --- | --- | --- | --- | --- |
| `27:56` | `1` | `70:4`, `80:2` | `70` | `70` reaches `thrC=4`, accept `70` | `70` | accept `70`; validate `70` |
| `27:56` | `5` | `70:4`, `80:2` | `70` | `70` reaches `thrC=4`, accept `70` | `70` | accept `70`; validate `70` |
| `27:56` | `2` | `70:3`, `80:3` (one `80` from Byzantine `p3`) | `70` | both reach `thrV=3`, neither reaches `thrC=4`; **`80` is the largest tied bucket**, so flip to `80` | `80` | |
| `27:57` | `4` | `60:1` (from `p3`), `70:3`, `80:2` | `80` | flip to `70`; `70` reaches `thrV=3`, but not `thrC=4` | `70` | |
| `27:57` | `0` | `70:2`, `80:4` | `80` | accept `80`; `80` reaches `thrC=4` | `80` | accept `80`; validate `80` |
| `27:57` | `2` | `70:3`, `80:3` | `80` | **threshold rises to `thrV=4`, no bucket reaches it**, so flip to `0` | `0` | |
| `27:57` | `3` | `0:1`, `70:3`, `80:2` | `80` | flip to `0`; no bucket reaches `thrV=4` | `0` | |
| `27:58` | `4` | `0:1`, `70:3`, `80:2` | `70` | flip to `0`; no bucket reaches `thrV=4` | `0` | |
| `27:58` | `6` | `0:3`, `70:3`, `80:1` | `70` | flip to `0`; no bucket reaches `thrV=4` | `0` | |
| `28:09-28:16` | `2,3,4,6` | all `0` | `0` | as time progresses, the non-zero proposals become **stale** and are removed; all remaining votes are `0`, which reaches the thresholds | `0` | accept `0`; `p2` and `p3` validate `0` |

At the end of the establish phase, the accepted ledgers are split `2:1:4`: `p1/p5`, `p0`, and `p2/p3/p4/p6` form three distinct branches. Validations are split `2:1:2`, so no branch reaches quorum and no ledger becomes fully validated. 

### Steps to Reproduce

The replay follows the same principle as Scenario 1: it fixes timing and proposal-delivery order, registers the extracted events in CSF, and lets the original rippled consensus logic compute the resulting proposals, accepts, and validations.

In addition to the parameters listed in Scenario 1, reproducing the close-time failure also requires fixing the following close-time-specific parameters:

- `initialCloseTime[i]`: the raw close time used by node `i` when it enters the establish phase. This determines the node's initial rounded close-time bucket.
- `ProposalDelivery.closeTime`: the close time carried by each delivered proposal. This determines how the receiver counts that proposal in its local `closeTimeVotes`.
- Byzantine close-time mutations: the replay delivers the receiver-specific `closeTime` values recorded for Byzantine node `p3`.

With these additional parameters fixed, CSF replays the same event schedule as in Scenario 1. Honest deliveries still require a matching proposal emitted by the original consensus logic. If no matching proposal was emitted, the replay may inject the trace proposal only when its sender is Byzantine node `p3`. The replay succeeds only if the final accepted-ledger and validation split matches the raw execution.

For example, the used values in this scenario include:
- `roundStartUs[i]`: `roundStartUs[0]` = 22037133, `roundStartUs[1]` = 20334188, ...
- `establishUs[i]`: `establishUs[0]` = 22037708, `establishUs[1]` = 20955307, ...
- `prevRoundTimeMs[i]`: `prevRoundTimeMs[0]` = 2001, `prevRoundTimeMs[1]` = 2001, ...
- `initialCloseTime[i]`: `initialCloseTime[0]` = 834276475, `initialCloseTime[1]` = 834276474, ...
- `TimerTick[i][k]`: `TimerTick[0][0]` = 24014595, ...
- `ProposalDelivery` = [{atUs: 21026697, sender: 1, receiver: 4, proposalSeq: 0, positionHash: 00000000…, closeTime: 834276474}, ...]

The receiver-specific close times sent by Byzantine node `p3` at `proposalSeq = 0` are, for example: `p3 -> p0` = 834276484, `p3 -> p1` = 834276474, `p3 -> p4` = 834276464.