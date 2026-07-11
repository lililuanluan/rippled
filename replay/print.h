#ifndef REPLAY_PRINT_H
#define REPLAY_PRINT_H

#include "trace.h"

#include <cstddef>
#include <ostream>
#include <vector>

namespace replay_trace {
namespace detail {

template <class T>
inline std::ostream&
printVector(std::ostream& os, std::vector<T> const& values)
{
    os << '[';
    for (std::size_t i = 0; i < values.size(); ++i)
    {
        if (i != 0)
            os << ", ";
        os << values[i];
    }
    os << ']';
    return os;
}

inline char const*
boolText(bool value)
{
    return value ? "true" : "false";
}

}  // namespace detail

inline std::ostream&
operator<<(std::ostream& os, TraceNode const& node)
{
    return os << "TraceNode{id=" << node.id << ", roundStartUs=" << node.roundStartUs
              << ", establishUs=" << node.establishUs
              << ", prevRoundTimeMs=" << node.prevRoundTimeMs
              << ", prevProposers=" << node.prevProposers << ", initialPositionHash=\""
              << node.initialPositionHash << '"' << ", initialCloseTime=" << node.initialCloseTime
              << ", prevCloseTime=" << node.prevCloseTime << '}';
}

inline std::ostream&
operator<<(std::ostream& os, ProposalDelivery const& delivery)
{
    return os << "ProposalDelivery{atUs=" << delivery.atUs << ", sender=" << delivery.sender
              << ", receiver=" << delivery.receiver << ", proposalSeq=" << delivery.proposalSeq
              << ", positionHash=\"" << delivery.positionHash << '"'
              << ", closeTime=" << delivery.closeTime << '}';
}

inline std::ostream&
operator<<(std::ostream& os, TimerTick const& tick)
{
    return os << "TimerTick{atUs=" << tick.atUs << ", node=" << tick.node
              << ", observedValidated=" << tick.observedValidated << '}';
}

inline std::ostream&
operator<<(std::ostream& os, TxSetMembership const& membership)
{
    return os << "TxSetMembership{txsetHash=\"" << membership.txsetHash << '"' << ", txHash=\""
              << membership.txHash << "\", present=" << detail::boolText(membership.present) << '}';
}

inline std::ostream&
operator<<(std::ostream& os, Accept const& accept)
{
    return os << "Accept{node=" << accept.node << ", ledgerHash=\"" << accept.ledgerHash
              << "\", txsetHash=\"" << accept.txsetHash << "\"}";
}

inline std::ostream&
operator<<(std::ostream& os, Validation const& validation)
{
    return os << "Validation{node=" << validation.node << ", ledgerHash=\"" << validation.ledgerHash
              << "\"}";
}

inline std::ostream&
operator<<(std::ostream& os, Unl const& unl)
{
    os << "Unl{node=" << unl.node << ", trusted=";
    detail::printVector(os, unl.trusted);
    return os << '}';
}

inline std::ostream&
operator<<(std::ostream& os, TraceData const& trace)
{
    os << "TraceData{\n";
    os << "  targetSeq=" << trace.targetSeq << ",\n";
    os << "  byzantineNodes=";
    detail::printVector(os, trace.byzantineNodes);
    os << ",\n";

    os << "  unl=[\n";
    for (auto const& item : trace.unl)
        os << "    " << item << ",\n";
    os << "  ],\n";

    os << "  nodes=[\n";
    for (auto const& item : trace.nodes)
        os << "    " << item << ",\n";
    os << "  ],\n";

    os << "  deliveries=[\n";
    for (auto const& item : trace.deliveries)
        os << "    " << item << ",\n";
    os << "  ],\n";

    os << "  txsetMemberships=[\n";
    for (auto const& item : trace.txsetMemberships)
        os << "    " << item << ",\n";
    os << "  ],\n";

    os << "  ticks=[\n";
    for (auto const& item : trace.ticks)
        os << "    " << item << ",\n";
    os << "  ],\n";

    os << "  accepts=[\n";
    for (auto const& item : trace.accepts)
        os << "    " << item << ",\n";
    os << "  ],\n";

    os << "  validations=[\n";
    for (auto const& item : trace.validations)
        os << "    " << item << ",\n";
    os << "  ]\n";
    return os << '}';
}

}  // namespace replay_trace

#endif  // REPLAY_PRINT_H
