

#include "test/csf.h"
#include "test/csf/Proposal.h"
#include "test/csf/Tx.h"
#include "test/csf/Validation.h"
#include "test/csf/events.h"

#include "trace.h"

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <map>
#include <string>
#include <vector>
using namespace xrpl::test::csf;
using namespace std::chrono;

void
example()
{
    Sim sim;
    // PeerGroup就是一个peer指针的vector，维护了一堆方法，例如trust
    PeerGroup validators = sim.createGroup(5);  // 创建5个节点
    PeerGroup center = sim.createGroup(1);      // 创建中心的hub节点用于通信

    PeerGroup network =
        validators + center;  // set union，返回新的数组，但是是指针数组，所以还是原来的节点

    center[0]->runAsValidator = false;

    // Sim 包含TrustGraph和BasicNetwork字段可以访问，来配置UNL和overlay网络
    validators.trust(validators);  // 全连接UNL
    center.trust(validators);
    // 这两行可以写成一行：network.trust(validators)

    SimDuration delay = 200ms;
    // 这里是配置overlay网络，所有validators连接到center这个hub，delay为200ms
    // 所有的连接都是双向的
    // delay就是普通的消息延迟
    validators.connect(center, delay);

    SimDurationCollector simDur;  // 是用于收集数据并分析的
    // 注册一个collector，一个collector是任意的实现了 on(NodeID, SimTime, Event)的类
    // collector注册是传入一个引用，其生命周期不依赖Sim
    sim.collectors.add(simDur);

    sim.run(1);  // 模拟直到每个node都生成一个新的ledger，这里类似预热状态

    // 提交transaction，这里显式提交到某个节点（模拟器没有client概念，向哪个node提交就
    // node->submit）
    for (Peer* p : validators)
    {
        p->submit(
            Tx(static_cast<std::uint32_t>(p->id)));  // transaction模拟为整数，这里就用id来表示
    }

    sim.run(1);

    // 获取simDur收集的数据，这里它收集模拟用时
    std::cout << "Simulated for " << duration_cast<milliseconds>(simDur.stop - simDur.start).count()
              << " ms" << std::endl;

    assert(sim.synchronized());
}

void
transaction()
{
    Tx const tx1{1};
    Tx const tx2{2};
    std::cout << "disputed: " << tx1 << std::endl;
    std::cout << "disputed: " << tx2 << std::endl;

    // 构造transaction set
    TxSet has12{TxSetType{tx1, tx2}};
    std::cout << "transaction set: " << has12.txs() << std::endl;
    TxSet has1{TxSetType{tx1}};
    std::cout << "transaction set: " << has1.txs() << std::endl;
}

void
scheduler()
{
    Scheduler sched;
    std::vector<std::string> log;

    // in是在1秒后执行，是相对时间，而at是在绝对时间上执行，这些执行都是在时间点而不是时间段上的
    // 调用in或者at之后，会将事件推入队列，然后step按照顺序执行，每次只处理下一个事件

    // 如果不推进，则scheduler的当前时间不变
    auto start = sched.now();
    // std::cout << "[start] scheduler now = " << start << std::endl; //
    // 这是一个timepoint，不能打印，只能打印 time_since_epoch() 等 scheduler主要关心相对start的时间

    // in，相对sched.now()的时间
    sched.in(1s, [&] {
        log.push_back("event A at +1 s");
        std::cout << "evnet A scheduled" << std::endl;
    });
    sched.in(3s, [&] {
        log.push_back("event B at +3 s");
        std::cout << "evnet B scheduled" << std::endl;
    });

    // at，绝对时间，通常用start+xxx来计算
    sched.at(start + 2s, [&] {
        log.push_back("event C at +2 s");
        std::cout << "evnet C scheduled" << std::endl;
    });

    // 现在队列里已经有一些事件了

    // stepOne 跑到下一个事件
    sched.stepOne();
    // 看看scheduler现在的时间：
    std::cout << "after stepOne, now = " << duration_cast<seconds>(sched.now() - start)
              << std::endl;  // after stepOne, now = 1s

    // stepFor(x) 向前推进x时间，把这之间的事件都执行了
    // step() 把剩下所有事件都跑完
    sched.step();

    std::cout << " --- logs ---\n";
    for (auto&& l : log)
    {
        std::cout << l << std::endl;
    }
}

struct ProposalCollector
{
    std::map<PeerID, std::vector<Share<Proposal>>>
        proposalShares;  // share是发起广播，Receive是接收，Relay是传递
    std::map<PeerID, std::vector<AcceptLedger>>
        accepts;  // accept之后，如果compatible且!movedon，就广播这个validation，这个先存起来，其他节点收到validation的时候，还需要accquire这个ledger才会更新trie
    std::map<PeerID, std::vector<Share<Validation>>> validationShares;

    // 这里是默认，后面特化定义不同的行为
    template <class Event>
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
};

void
thresholdChange()
{
    Sim sim;
    sim.sink.threshold(beast::Severity::Trace);  // 打开jlog
    ProposalCollector collector;
    sim.collectors.add(collector);

    PeerGroup peers = sim.createGroup(7);
    peers.trust(peers);

    Tx disputed{1};
    TxSet yesSet{TxSetType{disputed}};
    TxSet noSet{TxSetType{}};

    Peer* p = peers[5];

    p->openTxs.insert(disputed);
    p->fakeSetPreviousRound(2001ms, 6);
    std::cout << " == starting round ==\n";
    p->startRound();
    std::cout << " == handling noSet ==\n";
    p->handle(
        noSet);  // 将txs插入自己的txSet，然后调用consensus.gotTxSet()，这里是自己的初始投票（看见了就是yes）

    auto prevL = p->lastClosedLedger.id();
    // 构造proposal需要closetime，是proposal消息体内的时间戳，而seentime是这个节点第一次看到这个proposal的时间，用于后续stale判断
    auto closeTime = p->now();
    auto seenTime = p->now();

    auto makeProposal = [&](std::size_t from_peer, TxSet const& txSet) {
        return Proposal{
            prevL,
            Proposal::kSeqJoin,  // 就是刚join的时候的seq值，为0，后续改变position则为1，2，3...
                                 // 老版本是seqJoin，新版本是kSeqJoin
            txSet.id(),
            closeTime,
            seenTime,
            PeerID{static_cast<std::uint32_t>(from_peer)}};
    };

    // 模拟node5收到其他节点的初始proposal
    for (std::size_t peer : {0, 1, 2})
    {
        std::cout << "== handling peer proposal from " << peer << " ==\n";
        p->handle(makeProposal(peer, yesSet));
    }
    for (std::size_t peer : {3, 4, 6})
    {
        std::cout << "== handling peer proposal from " << peer << " ==\n";
        p->handle(makeProposal(peer, noSet));
    }

    auto steps = std::array{0ms, 3001ms, 1001ms};
    for (int i = 0; i < 3; i++)
    {
        sim.scheduler.stepFor(steps[i]);
        std::cout << "== calling timer entry ==\n";
        p->timerEntryOnce();
        auto& proposed = collector.proposalShares[p->id].back().val.position();

        std::cout << "after " << i
                  << "-th time entry, node5 proposes: " << (proposed == yesSet.id() ? "yes" : "no")
                  << std::endl;
    }
}

int
main()
{
    // example();
    // transaction();
    // scheduler();
    thresholdChange();
}
