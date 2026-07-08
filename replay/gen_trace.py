#!/usr/bin/env python3
"""Extract a deterministic CSF replay trace from raw Rocket/rippled logs.

The script is intentionally standalone: it only uses the Python standard
library and reads the raw files in a Rocket case directory directly.  The
resulting JSON can be consumed by luan/main.cpp.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_TARGET_SEQ = 9
DEFAULT_TARGET_TX = "A3F3D68753A7BD06B48A1ABE71FE0B6CD4C1B31B10FCAA33B91FFEECCB648936"
DEFAULT_HAS_TARGET_TXSET = "D136B9977B679B9DF5992CA2307755C76AE85B117DAC00FEFBEB34E4B0183F75"

TIMESTAMP_RE = re.compile(
    r"^(\d{4}-[A-Za-z]{3}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ UTC)\s"
)
VALIDATOR_LOG_RE = re.compile(r".*validator_(\d+)_(?:log|debug)\.txt$")
START_ROUND_RE = re.compile(
    r"startRoundInternal transitioned to ConsensusPhase::open, "
    r"previous ledgerID: ([A-F0-9]+), seq: (\d+)\. "
    r"number of peer proposals,previous proposers: (\d+),(\d+)\."
)
CLOSE_LEDGER_RE = re.compile(
    r"prevRoundTime: (\d+)ms, .*?closeLedger transitioned to ConsensusPhase::establish"
)
CREATE_DISPUTES_RE = re.compile(r"createDisputes ([A-F0-9]+) to ([A-F0-9]+)")
PEER_VOTE_RE = re.compile(r"Peer ([A-F0-9]+) votes (YES|NO) on ([A-F0-9]+)")
PEER_NOW_VOTE_RE = re.compile(r"Peer ([A-F0-9]+) now votes (YES|NO) on ([A-F0-9]+)")
NO_CHANGE_RE = re.compile(
    r"No change \((YES|NO)\) on ([A-F0-9]+) : weight (-?\d+), "
    r"percent (\d+), round\(s\) with this vote: (\d+)"
)
WE_NOW_RE = re.compile(r"We now vote (YES|NO) on ([A-F0-9]+)")
JSON_VOTE_RE = re.compile(r"LedgerConsensus:DBG (\{.*\})$")
TIMER_RE = re.compile(r"ConsensusLogger Heartbeat Timer:")
CONVERGE_RE = re.compile(
    r"convergePercent_ (\d+) is based on round duration so far: (\d+)ms, "
    r"previous round duration: (\d+)ms, avMIN_CONSENSUS_TIME: (\d+)ms"
)
NEEDED_WEIGHT_RE = re.compile(r"neededWeight (\d+)|Proposers:\d+ nw:(\d+)")
CHECK_CONSENSUS_RE = re.compile(
    r"checkConsensus: prop=(\d+)/(\d+) agree=(\d+) validated=(\d+) time=(\d+)/(\d+)"
)
POSITION_CHANGE_RE = re.compile(r"Position change: CTime (\d+), tx ([A-F0-9]+)")
BUILDING_SET_RE = re.compile(r"Building canonical tx set: ([A-F0-9]+)")
BUILT_LEDGER_RE = re.compile(r"Built ledger #(\d+): ([A-F0-9]+)")
VALIDATION_ID_RE = re.compile(r"node_id: ([A-F0-9]{40}).*?master_key: (\S+)")
PROPOSAL_RE = re.compile(
    r"PROPOSAL proposal: previous_ledger: ([A-F0-9]+) "
    r"proposal_seq: (\d+) position: ([A-F0-9]+) "
    r"close_time: (.*?) now: .*? is_bow_out:(\d+) node_id: ([A-F0-9]{40})"
)

AV_MIN_ROUNDS = 2
AVALANCHE_CUTOFFS = {
    "init": {"time": 0, "pct": 50, "next": "mid"},
    "mid": {"time": 50, "pct": 65, "next": "late"},
    "late": {"time": 85, "pct": 70, "next": "stuck"},
    "stuck": {"time": 200, "pct": 95, "next": "stuck"},
}


@dataclass
class OpenRound:
    working_seq: int
    prev_ledger: str
    start_timestamp: str
    start_ms: int
    source_line: int
    current_peer_proposals: int
    previous_proposers: int


@dataclass
class PendingDispute:
    timestamp: str
    timestamp_ms: int
    source_line: int
    local_txset: str
    other_txset: str
    peer_votes: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PendingVoteUpdate:
    timestamp: str
    timestamp_ms: int
    source_line: int
    tx: str
    action: str
    old_vote: str | None
    new_vote: str
    weight: int | None = None
    percent: int | None = None
    rounds_with_vote: int | None = None
    json_state: dict[str, Any] | None = None


def parse_timestamp(timestamp: str) -> datetime:
    match = re.match(
        r"(\d{4}-[A-Za-z]{3}-\d{2} \d{2}:\d{2}:\d{2})\.(\d+) UTC$",
        timestamp,
    )
    if not match:
        return datetime.strptime(timestamp, "%Y-%b-%d %H:%M:%S UTC").replace(
            tzinfo=timezone.utc
        )
    whole = match.group(1)
    fraction = (match.group(2) + "000000")[:6]
    dt = datetime.strptime(whole, "%Y-%b-%d %H:%M:%S")
    return dt.replace(microsecond=int(fraction), tzinfo=timezone.utc)


def timestamp_ms(timestamp: str) -> int:
    return int(parse_timestamp(timestamp).timestamp() * 1000)


def timestamp_us(timestamp: str) -> int:
    return int(parse_timestamp(timestamp).timestamp() * 1_000_000)


def timestamp(line: str) -> str | None:
    match = TIMESTAMP_RE.match(line)
    return None if match is None else match.group(1)


def node_from_log(path: Path) -> int | None:
    match = VALIDATOR_LOG_RE.fullmatch(path.name)
    return None if match is None else int(match.group(1))


def resolve_iteration_dir(case_dir: Path) -> Path:
    if case_dir.name.startswith("iteration-") and case_dir.is_dir():
        return case_dir
    direct = case_dir / "iteration-1"
    if direct.is_dir():
        return direct
    candidates = sorted(p for p in case_dir.glob("iteration-*") if p.is_dir())
    if candidates:
        return candidates[0]
    raise FileNotFoundError(f"no iteration-* directory found under {case_dir}")


def select_logs(iteration_dir: Path, *, debug: bool) -> dict[int, Path]:
    live_dir = iteration_dir / "validator_live_logs"
    suffix = "debug" if debug else "log"
    out: dict[int, Path] = {}
    if live_dir.is_dir():
        for path in sorted(live_dir.glob(f"validator_*_{suffix}.txt")):
            node = node_from_log(path)
            if node is not None:
                out[node] = path
    if out:
        return out

    fallback = iteration_dir / "validator_logs"
    if fallback.is_dir():
        for path in sorted(fallback.glob("*validator_*_log.txt")):
            node = node_from_log(path)
            if node is not None:
                out[node] = path
    if out:
        return out
    raise FileNotFoundError(f"no validator logs found in {iteration_dir}")


def load_node_info(iteration_dir: Path) -> tuple[dict[str, int], dict[int, dict[str, str]]]:
    paths = sorted(iteration_dir.glob("node_info-*.csv"))
    if not paths:
        raise FileNotFoundError(f"no node_info-*.csv found in {iteration_dir}")

    base58_to_node: dict[str, int] = {}
    nodes: dict[int, dict[str, str]] = {}
    with paths[0].open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            node = int(row["node_id"])
            public_key = row["public_key"]
            base58_to_node[public_key] = node
            nodes[node] = {
                "public_key": public_key,
                "private_key": row.get("private_key", ""),
            }
    return base58_to_node, nodes


def load_hex_node_map(iteration_dir: Path, base58_to_node: dict[str, int]) -> dict[str, int]:
    hex_to_node: dict[str, int] = {}
    for directory in (iteration_dir / "validator_live_logs", iteration_dir / "validator_logs"):
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob("*.txt")):
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                match = VALIDATION_ID_RE.search(line)
                if not match:
                    continue
                node = base58_to_node.get(match.group(2))
                if node is not None:
                    hex_to_node[match.group(1)] = node
    return hex_to_node


def vote_to_bool(vote: str) -> bool:
    return vote.upper() == "YES"


def compute_weight(yays: int, nays: int, our_vote: bool) -> int:
    return (yays * 100 + (100 if our_vote else 0)) // (yays + nays + 1)


def dispute_required_pct(
    current_state: str,
    percent_time: int | None,
    current_rounds: int,
) -> tuple[int | None, str | None]:
    if percent_time is None:
        return None, None
    current = AVALANCHE_CUTOFFS[current_state]
    next_state = str(current["next"])
    if next_state != current_state and current_rounds >= AV_MIN_ROUNDS:
        next_cutoff = AVALANCHE_CUTOFFS[next_state]
        if percent_time >= int(next_cutoff["time"]):
            return int(next_cutoff["pct"]), next_state
    return int(current["pct"]), None


def nearest_round(open_rounds: list[OpenRound], timestamp_ms_: int, target_seq: int) -> OpenRound | None:
    candidates = [r for r in open_rounds if r.start_ms <= timestamp_ms_]
    if not candidates:
        return None
    active = max(candidates, key=lambda r: r.start_ms)
    return active if active.working_seq == target_seq else None


def scan_consensus_logs(
    log_files: dict[int, Path],
    *,
    target_seq: int,
    target_tx: str,
    hex_to_node: dict[str, int],
) -> dict[str, Any]:
    node_data: dict[int, dict[str, Any]] = {
        node: {
            "rounds": [],
            "establish": [],
            "disputes": [],
            "peer_vote_updates": [],
            "vote_updates": [],
            "timer_events": [],
            "position_changes": [],
            "built_ledgers": [],
        }
        for node in sorted(log_files)
    }

    for node, path in sorted(log_files.items()):
        active_rounds: list[OpenRound] = []
        dispute_state = {
            "avalanche_state": "init",
            "avalanche_counter": 0,
            "current_vote_counter": 0,
        }
        pending_dispute: PendingDispute | None = None
        pending_update: PendingVoteUpdate | None = None

        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line_no, line in enumerate(lines, start=1):
            ts = timestamp(line)
            if not ts:
                continue
            ts_ms = timestamp_ms(ts)

            start_match = START_ROUND_RE.search(line)
            if start_match:
                prev_seq = int(start_match.group(2))
                round_info = OpenRound(
                    working_seq=prev_seq + 1,
                    prev_ledger=start_match.group(1),
                    start_timestamp=ts,
                    start_ms=ts_ms,
                    source_line=line_no,
                    current_peer_proposals=int(start_match.group(3)),
                    previous_proposers=int(start_match.group(4)),
                )
                active_rounds.append(round_info)
                if round_info.working_seq == target_seq:
                    node_data[node]["rounds"].append(
                        {
                            "working_seq": round_info.working_seq,
                            "prev_ledger": round_info.prev_ledger,
                            "start_timestamp": ts,
                            "start_ms": ts_ms,
                            "current_peer_proposals": round_info.current_peer_proposals,
                            "previous_proposers": round_info.previous_proposers,
                            "source": f"{path.name}:{line_no}",
                        }
                    )
                continue

            close_match = CLOSE_LEDGER_RE.search(line)
            if close_match:
                active = nearest_round(active_rounds, ts_ms, target_seq)
                if active is not None:
                    node_data[node]["establish"].append(
                        {
                            "working_seq": target_seq,
                            "timestamp": ts,
                            "time_ms": ts_ms,
                            "relative_to_round_start_ms": ts_ms - active.start_ms,
                            "prev_round_time_ms": int(close_match.group(1)),
                            "source": f"{path.name}:{line_no}",
                        }
                    )

            dispute_match = CREATE_DISPUTES_RE.search(line)
            if dispute_match:
                active = nearest_round(active_rounds, ts_ms, target_seq)
                if active is not None:
                    pending_dispute = PendingDispute(
                        timestamp=ts,
                        timestamp_ms=ts_ms,
                        source_line=line_no,
                        local_txset=dispute_match.group(1),
                        other_txset=dispute_match.group(2),
                    )
                continue

            if pending_dispute is not None:
                peer_vote_match = PEER_VOTE_RE.search(line)
                if peer_vote_match and peer_vote_match.group(3) == target_tx:
                    overlay = peer_vote_match.group(1)
                    pending_dispute.peer_votes.append(
                        {
                            "node_id_hex": overlay,
                            "node": hex_to_node.get(overlay),
                            "vote": peer_vote_match.group(2).lower(),
                        }
                    )
                    continue
                if "differences found" in line:
                    yes_nodes = sorted(
                        v["node"]
                        for v in pending_dispute.peer_votes
                        if v["vote"] == "yes" and v["node"] is not None
                    )
                    no_nodes = sorted(
                        v["node"]
                        for v in pending_dispute.peer_votes
                        if v["vote"] == "no" and v["node"] is not None
                    )
                    node_data[node]["disputes"].append(
                        {
                            "working_seq": target_seq,
                            "timestamp": pending_dispute.timestamp,
                            "time_ms": pending_dispute.timestamp_ms,
                            "local_txset": pending_dispute.local_txset,
                            "other_txset": pending_dispute.other_txset,
                            "target_tx": target_tx,
                            "peer_votes": pending_dispute.peer_votes,
                            "yes_nodes": yes_nodes,
                            "no_nodes": no_nodes,
                            "source": f"{path.name}:{pending_dispute.source_line}",
                        }
                    )
                    pending_dispute = None
                    continue

            peer_now_match = PEER_NOW_VOTE_RE.search(line)
            if peer_now_match and peer_now_match.group(3) == target_tx:
                overlay = peer_now_match.group(1)
                node_data[node]["peer_vote_updates"].append(
                    {
                        "working_seq": target_seq,
                        "timestamp": ts,
                        "time_ms": ts_ms,
                        "peer_node_id_hex": overlay,
                        "peer_node": hex_to_node.get(overlay),
                        "vote": peer_now_match.group(2).lower(),
                        "source": f"{path.name}:{line_no}",
                    }
                )
                continue

            no_change_match = NO_CHANGE_RE.search(line)
            if no_change_match and no_change_match.group(2) == target_tx:
                vote = no_change_match.group(1).lower()
                pending_update = PendingVoteUpdate(
                    timestamp=ts,
                    timestamp_ms=ts_ms,
                    source_line=line_no,
                    tx=target_tx,
                    action="no_change",
                    old_vote=vote,
                    new_vote=vote,
                    weight=int(no_change_match.group(3)),
                    percent=int(no_change_match.group(4)),
                    rounds_with_vote=int(no_change_match.group(5)),
                )
                continue

            we_now_match = WE_NOW_RE.search(line)
            if we_now_match and we_now_match.group(2) == target_tx:
                new_vote = we_now_match.group(1).lower()
                pending_update = PendingVoteUpdate(
                    timestamp=ts,
                    timestamp_ms=ts_ms,
                    source_line=line_no,
                    tx=target_tx,
                    action="changed",
                    old_vote=None,
                    new_vote=new_vote,
                )
                continue

            if TIMER_RE.search(line):
                active = nearest_round(active_rounds, ts_ms, target_seq)
                if active is not None and "Phase establish." in line:
                    converge_match = CONVERGE_RE.search(line)
                    needed_matches = NEEDED_WEIGHT_RE.findall(line)
                    consensus_match = CHECK_CONSENSUS_RE.search(line)
                    required_pct = None
                    for a, b in needed_matches:
                        if a or b:
                            required_pct = int(a or b)
                            break

                    establish = (
                        node_data[node]["establish"][-1]
                        if node_data[node]["establish"]
                        else None
                    )
                    event_timestamp = ts
                    if pending_update is not None and "updateOurPositions." in line:
                        event_timestamp = pending_update.timestamp
                    event_ms = timestamp_ms(event_timestamp)
                    node_data[node]["timer_events"].append(
                        {
                            "working_seq": target_seq,
                            "timestamp": event_timestamp,
                            "heartbeat_timestamp": ts,
                            "time_ms": event_ms,
                            "relative_to_establish_ms": None
                            if establish is None
                            else event_ms - int(establish["time_ms"]),
                            "relative_to_round_start_ms": event_ms - active.start_ms,
                            "has_update_our_positions": "updateOurPositions." in line,
                            "accepted": "ConsensusPhase::accepted" in line
                            or "phase establish changed to accepted" in line,
                            "round_time_ms": None
                            if converge_match is None
                            else int(converge_match.group(2)),
                            "prev_round_time_ms": None
                            if converge_match is None
                            else int(converge_match.group(3)),
                            "required_pct": required_pct,
                            "check_consensus": None
                            if consensus_match is None
                            else {
                                "proposing": int(consensus_match.group(1)),
                                "total": int(consensus_match.group(2)),
                                "agree": int(consensus_match.group(3)),
                                "validated": int(consensus_match.group(4)),
                                "time_ms": int(consensus_match.group(5)),
                                "previous_time_ms": int(consensus_match.group(6)),
                            },
                            "source": f"{path.name}:{line_no}",
                        }
                    )

            if pending_update is not None:
                json_match = JSON_VOTE_RE.search(line)
                if json_match:
                    try:
                        pending_update.json_state = json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        pass
                    continue

                if TIMER_RE.search(line) and "updateOurPositions." in line:
                    converge_match = CONVERGE_RE.search(line)
                    needed_matches = NEEDED_WEIGHT_RE.findall(line)
                    consensus_match = CHECK_CONSENSUS_RE.search(line)
                    required_pct = None
                    for a, b in needed_matches:
                        if a or b:
                            required_pct = int(a or b)
                            break
                    percent_time = (
                        pending_update.percent
                        if pending_update.percent is not None
                        else (None if converge_match is None else int(converge_match.group(1)))
                    )
                    yays = int((pending_update.json_state or {}).get("yays", 0))
                    nays = int((pending_update.json_state or {}).get("nays", 0))
                    our_vote = bool((pending_update.json_state or {}).get("our_vote", False))
                    weight = pending_update.weight
                    if weight is None and (yays or nays):
                        old_vote_bool = not our_vote if pending_update.action == "changed" else our_vote
                        weight = compute_weight(yays, nays, old_vote_bool)
                        pending_update.old_vote = "yes" if old_vote_bool else "no"
                    elif pending_update.old_vote is None:
                        pending_update.old_vote = pending_update.new_vote

                    state_before = str(dispute_state["avalanche_state"])
                    counter_before = int(dispute_state["avalanche_counter"])
                    counter_used = counter_before + 1
                    dispute_pct, new_state = dispute_required_pct(
                        state_before, percent_time, counter_used
                    )
                    if new_state is None:
                        dispute_state["avalanche_counter"] = counter_used
                    else:
                        dispute_state["avalanche_state"] = new_state
                        dispute_state["avalanche_counter"] = 0
                    if pending_update.action == "no_change":
                        dispute_state["current_vote_counter"] = (
                            int(dispute_state["current_vote_counter"]) + 1
                        )
                    else:
                        dispute_state["current_vote_counter"] = 0

                    active = nearest_round(active_rounds, pending_update.timestamp_ms, target_seq)
                    establish = node_data[node]["establish"][-1] if node_data[node]["establish"] else None
                    vote_row = {
                        "working_seq": target_seq,
                        "timestamp": pending_update.timestamp,
                        "time_ms": pending_update.timestamp_ms,
                        "relative_to_establish_ms": None
                        if establish is None
                        else pending_update.timestamp_ms - int(establish["time_ms"]),
                        "relative_to_round_start_ms": None
                        if active is None
                        else pending_update.timestamp_ms - active.start_ms,
                        "tx": target_tx,
                        "action": pending_update.action,
                        "old_vote": pending_update.old_vote,
                        "new_vote": pending_update.new_vote,
                        "yays": yays,
                        "nays": nays,
                        "our_vote_after": (pending_update.json_state or {}).get("our_vote"),
                        "weight": weight,
                        "percent": percent_time,
                        "round_time_ms": None if converge_match is None else int(converge_match.group(2)),
                        "prev_round_time_ms": None if converge_match is None else int(converge_match.group(3)),
                        "av_min_consensus_time_ms": None
                        if converge_match is None
                        else int(converge_match.group(4)),
                        "required_pct": dispute_pct,
                        "avalanche_state_before": state_before,
                        "avalanche_state_after": dispute_state["avalanche_state"],
                        "avalanche_counter_before": counter_before,
                        "avalanche_counter_used": counter_used,
                        "avalanche_counter_after": dispute_state["avalanche_counter"],
                        "current_vote_counter_after": dispute_state["current_vote_counter"],
                        "close_time_needed_weight": required_pct,
                        "check_consensus": None
                        if consensus_match is None
                        else {
                            "proposing": int(consensus_match.group(1)),
                            "total": int(consensus_match.group(2)),
                            "agree": int(consensus_match.group(3)),
                            "validated": int(consensus_match.group(4)),
                            "time_ms": int(consensus_match.group(5)),
                            "previous_time_ms": int(consensus_match.group(6)),
                        },
                        "votes": (pending_update.json_state or {}).get("votes", {}),
                        "source": f"{path.name}:{pending_update.source_line}",
                    }
                    node_data[node]["vote_updates"].append(vote_row)
                    if (
                        node_data[node]["timer_events"]
                        and node_data[node]["timer_events"][-1].get("heartbeat_timestamp") == ts
                        and node_data[node]["timer_events"][-1].get("source") == f"{path.name}:{line_no}"
                    ):
                        node_data[node]["timer_events"][-1].update(
                            {
                                "tx": vote_row["tx"],
                                "action": vote_row["action"],
                                "old_vote": vote_row["old_vote"],
                                "new_vote": vote_row["new_vote"],
                                "yays": vote_row["yays"],
                                "nays": vote_row["nays"],
                                "our_vote_after": vote_row["our_vote_after"],
                                "weight": vote_row["weight"],
                                "percent": vote_row["percent"],
                                "required_pct": vote_row["required_pct"],
                                "avalanche_state_before": vote_row["avalanche_state_before"],
                                "avalanche_state_after": vote_row["avalanche_state_after"],
                                "avalanche_counter_before": vote_row["avalanche_counter_before"],
                                "avalanche_counter_used": vote_row["avalanche_counter_used"],
                                "avalanche_counter_after": vote_row["avalanche_counter_after"],
                                "current_vote_counter_after": vote_row["current_vote_counter_after"],
                                "close_time_needed_weight": vote_row["close_time_needed_weight"],
                                "votes": vote_row["votes"],
                                "vote_source": vote_row["source"],
                            }
                        )
                    pending_update = None
                    continue

            position_match = POSITION_CHANGE_RE.search(line)
            if position_match:
                active = nearest_round(active_rounds, ts_ms, target_seq)
                if active is not None:
                    node_data[node]["position_changes"].append(
                        {
                            "working_seq": target_seq,
                            "timestamp": ts,
                            "time_ms": ts_ms,
                            "close_time": int(position_match.group(1)),
                            "txset": position_match.group(2),
                            "source": f"{path.name}:{line_no}",
                        }
                    )

            build_match = BUILDING_SET_RE.search(line)
            if build_match:
                active = nearest_round(active_rounds, ts_ms, target_seq)
                if active is not None:
                    node_data[node]["_pending_build_set"] = build_match.group(1)

            built_match = BUILT_LEDGER_RE.search(line)
            if built_match and int(built_match.group(1)) == target_seq:
                node_data[node]["built_ledgers"].append(
                    {
                        "working_seq": target_seq,
                        "timestamp": ts,
                        "time_ms": ts_ms,
                        "txset": node_data[node].pop("_pending_build_set", ""),
                        "ledger": built_match.group(2),
                        "source": f"{path.name}:{line_no}",
                    }
                )

    for data in node_data.values():
        data.pop("_pending_build_set", None)
    return {"nodes": {str(node): data for node, data in sorted(node_data.items())}}


def scan_proposal_deliveries(
    debug_logs: dict[int, Path],
    *,
    target_prev_ledgers: set[str],
    hex_to_node: dict[str, int],
) -> list[dict[str, Any]]:
    deliveries: list[dict[str, Any]] = []
    for receiver, path in sorted(debug_logs.items()):
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line_no, line in enumerate(lines, start=1):
            ts = timestamp(line)
            if not ts:
                continue
            match = PROPOSAL_RE.search(line)
            if not match or match.group(1) not in target_prev_ledgers:
                continue
            sender_hex = match.group(6)
            if sender_hex not in hex_to_node:
                raise ValueError(
                    f"cannot map proposal node_id {sender_hex} in {path.name}:{line_no}"
                )
            deliveries.append(
                {
                    "timestamp": ts,
                    "time_ms": timestamp_ms(ts),
                    "time_us": timestamp_us(ts),
                    "sender": hex_to_node[sender_hex],
                    "sender_node_id_hex": sender_hex,
                    "receiver": receiver,
                    "proposal_seq": int(match.group(2)),
                    "txset": match.group(3),
                    "close_time": match.group(4),
                    "is_bow_out": match.group(5) == "1",
                    "source": f"{path.name}:{line_no}",
                }
            )
    return sorted(
        deliveries,
        key=lambda r: (int(r["time_us"]), int(r["receiver"]), int(r["sender"]), int(r["proposal_seq"])),
    )


def first_proposals(deliveries: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    initial: dict[int, dict[str, Any]] = {}
    for row in deliveries:
        if int(row["proposal_seq"]) == 0 and int(row["sender"]) not in initial:
            initial[int(row["sender"])] = dict(row)
    return initial


def proposal_updates(deliveries: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    updates: dict[int, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[int, int, str]] = set()
    for row in deliveries:
        key = (int(row["sender"]), int(row["proposal_seq"]), str(row["txset"]))
        if int(row["proposal_seq"]) == 0 or key in seen:
            continue
        seen.add(key)
        updates[int(row["sender"])].append(dict(row))
    return dict(sorted(updates.items()))


def accepted_split(nodes: dict[str, dict[str, Any]]) -> dict[str, list[int]]:
    split: dict[str, list[int]] = defaultdict(list)
    for node_text, data in nodes.items():
        built = data.get("built_ledgers") or []
        if not built:
            continue
        txset = built[0].get("txset") or ""
        split[txset].append(int(node_text))
    return {txset: sorted(members) for txset, members in sorted(split.items())}


def load_target_transaction(iteration_dir: Path, target_tx: str) -> dict[str, str] | None:
    paths = sorted(iteration_dir.glob("transaction-*.csv"))
    for path in paths:
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("tx_hash") == target_tx:
                    row["source"] = path.name
                    return row
    return None


def build_trace(
    case_dir: Path,
    *,
    target_seq: int,
    target_tx: str,
    has_target_txset: str,
) -> dict[str, Any]:
    case_dir = case_dir.expanduser().resolve()
    iteration_dir = resolve_iteration_dir(case_dir)
    base58_to_node, node_info = load_node_info(iteration_dir)
    hex_to_node = load_hex_node_map(iteration_dir, base58_to_node)

    log_files = select_logs(iteration_dir, debug=False)
    debug_logs = select_logs(iteration_dir, debug=True)
    consensus = scan_consensus_logs(
        log_files,
        target_seq=target_seq,
        target_tx=target_tx,
        hex_to_node=hex_to_node,
    )
    nodes = consensus["nodes"]
    target_prev_ledgers = {
        str(round_info["prev_ledger"])
        for node in nodes.values()
        for round_info in node.get("rounds", [])
    }
    if not target_prev_ledgers:
        raise ValueError(f"target seq {target_seq} was not found in consensus logs")

    deliveries = scan_proposal_deliveries(
        debug_logs,
        target_prev_ledgers=target_prev_ledgers,
        hex_to_node=hex_to_node,
    )
    if not deliveries:
        raise ValueError(f"no proposal deliveries found for seq {target_seq}")

    initial = first_proposals(deliveries)
    updates = proposal_updates(deliveries)
    split = accepted_split(nodes)
    all_txsets = {
        str(row.get("txset"))
        for row in deliveries
        if row.get("txset")
    }
    all_txsets.update(txset for txset in split if txset)
    if has_target_txset not in all_txsets:
        raise ValueError(f"--has-target-txset is not present in the trace: {has_target_txset}")
    opposite_txsets = sorted(txset for txset in all_txsets if txset != has_target_txset)
    if len(opposite_txsets) != 1:
        raise ValueError(
            "CSF replay currently supports one target txset and one opposite txset; "
            f"found opposite txsets: {opposite_txsets}"
        )

    def txset_has_target(txset: str) -> bool:
        if txset == has_target_txset:
            return True
        if txset in opposite_txsets:
            return False
        raise ValueError(f"unexpected txset: {txset}")

    timestamps: list[str] = []
    for node_data in nodes.values():
        for key in ("rounds", "establish", "vote_updates", "timer_events", "built_ledgers"):
            for row in node_data.get(key, []) or []:
                ts = row.get("start_timestamp") or row.get("timestamp")
                if ts:
                    timestamps.append(str(ts))
    timestamps.extend(str(row["timestamp"]) for row in deliveries)
    source_base_us = min(timestamp_us(ts) for ts in timestamps)

    def rel_us(ts: str) -> int:
        return timestamp_us(ts) - source_base_us

    accepted_by_node: dict[int, bool] = {}
    for txset, members in split.items():
        for node in members:
            accepted_by_node[int(node)] = txset_has_target(txset)

    node_rows: list[dict[str, Any]] = []
    for node in sorted(int(n) for n in nodes):
        node_text = str(node)
        node_data = nodes[node_text]
        rounds = node_data.get("rounds") or []
        establish = node_data.get("establish") or []
        if not rounds:
            raise ValueError(f"node {node} has no target round")
        if not establish:
            raise ValueError(f"node {node} has no establish transition")
        if node not in initial:
            raise ValueError(f"node {node} has no initial proposal")
        if node not in accepted_by_node:
            raise ValueError(f"node {node} has no accepted ledger for target seq")
        round_info = rounds[0]
        establish_info = establish[0]
        prev_round_ms = establish_info.get("prev_round_time_ms")
        if prev_round_ms is None:
            vote_updates = node_data.get("vote_updates") or []
            prev_round_ms = vote_updates[0].get("prev_round_time_ms") if vote_updates else None
        if prev_round_ms is None:
            raise ValueError(f"node {node} has no prevRoundTime")

        public_key = node_info.get(node, {}).get("public_key", "")
        hex_ids = sorted(hex_id for hex_id, mapped_node in hex_to_node.items() if mapped_node == node)
        built = (node_data.get("built_ledgers") or [{}])[0]
        node_rows.append(
            {
                "node": node,
                "public_key": public_key,
                "node_id_hex": hex_ids[0] if hex_ids else "",
                "round_start_us": rel_us(str(round_info["start_timestamp"])),
                "round_start_timestamp": round_info["start_timestamp"],
                "establish_us": rel_us(str(establish_info["timestamp"])),
                "establish_timestamp": establish_info["timestamp"],
                "prev_round_time_ms": int(prev_round_ms),
                "prev_proposers": int(round_info["previous_proposers"]),
                "initial_txset": initial[node]["txset"],
                "initial_has_target": txset_has_target(str(initial[node]["txset"])),
                "expected_txset": built.get("txset", ""),
                "expected_has_target": accepted_by_node[node],
                "round_source": round_info.get("source", ""),
                "establish_source": establish_info.get("source", ""),
                "accept_source": built.get("source", ""),
            }
        )

    delivery_rows = [
        {
            **row,
            "at_us": rel_us(str(row["timestamp"])),
            "has_target": txset_has_target(str(row["txset"])),
        }
        for row in deliveries
    ]

    tick_rows: list[dict[str, Any]] = []
    for node_text, node_data in sorted(nodes.items(), key=lambda item: int(item[0])):
        timer_events = node_data.get("timer_events") or []
        if not timer_events:
            timer_events = node_data.get("vote_updates") or []
        for tick_index, event in enumerate(timer_events, start=1):
            check = event.get("check_consensus") or {}
            tick_rows.append(
                {
                    "at_us": rel_us(str(event["timestamp"])),
                    "node": int(node_text),
                    "tick_index": tick_index,
                    "observed_validated": int(check.get("validated") or 0),
                    "timestamp": event["timestamp"],
                    "heartbeat_timestamp": event.get("heartbeat_timestamp"),
                    "round_time_ms": event.get("round_time_ms"),
                    "required_pct": event.get("required_pct"),
                    "weight": event.get("weight"),
                    "old_vote": event.get("old_vote"),
                    "new_vote": event.get("new_vote"),
                    "has_update_our_positions": event.get("has_update_our_positions"),
                    "accepted": event.get("accepted"),
                    "source": event.get("source", ""),
                }
            )
    tick_rows.sort(key=lambda r: (int(r["at_us"]), int(r["node"]), int(r["tick_index"])))

    accept_ticks: list[dict[str, Any]] = []
    for node_text, node_data in sorted(nodes.items(), key=lambda item: int(item[0])):
        built = node_data.get("built_ledgers") or []
        if built:
            accept_ticks.append(
                {
                    "at_us": rel_us(str(built[0]["timestamp"])),
                    "node": int(node_text),
                    "observed_validated": 0,
                    "timestamp": built[0]["timestamp"],
                    "source": built[0].get("source", ""),
                }
            )
    accept_ticks.sort(key=lambda r: (int(r["at_us"]), int(r["node"])))

    expected_keys: set[tuple[int, int, bool]] = set()
    for row in initial.values():
        expected_keys.add(
            (int(row["sender"]), int(row["proposal_seq"]), txset_has_target(str(row["txset"])))
        )
    for rows in updates.values():
        for row in rows:
            expected_keys.add(
                (int(row["sender"]), int(row["proposal_seq"]), txset_has_target(str(row["txset"])))
            )
    for row in deliveries:
        expected_keys.add(
            (int(row["sender"]), int(row["proposal_seq"]), txset_has_target(str(row["txset"])))
        )
    expected_proposals = [
        {"sender": sender, "proposal_seq": seq, "has_target": has_target}
        for sender, seq, has_target in sorted(expected_keys)
    ]

    initial_counts = Counter(str(row["txset"]) for row in initial.values())
    return {
        "schema": "luan.csf_trace.v1",
        "case_dir": str(case_dir),
        "iteration_dir": str(iteration_dir),
        "target_seq": target_seq,
        "target_tx": target_tx,
        "target_transaction": load_target_transaction(iteration_dir, target_tx),
        "source_base_unix_us": source_base_us,
        "has_target_txset": has_target_txset,
        "opposite_txset": opposite_txsets[0],
        "node_id_hex_to_node": dict(sorted(hex_to_node.items(), key=lambda item: item[1])),
        "initial_proposal_counts": dict(sorted(initial_counts.items())),
        "accepted_split_by_txset": split,
        "initial_proposals": {str(node): row for node, row in sorted(initial.items())},
        "proposal_updates": {str(node): rows for node, rows in sorted(updates.items())},
        "proposal_deliveries": delivery_rows,
        "consensus": consensus,
        "replay": {
            "nodes": node_rows,
            "deliveries": delivery_rows,
            "ticks": tick_rows,
            "accept_ticks": accept_ticks,
            "expected_proposals": expected_proposals,
        },
    }


def json_reader_safe(value: Any) -> Any:
    """Keep rippled's Json::Reader from rejecting large Unix timestamps."""
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if (
                isinstance(item, int)
                and (key.endswith("_ms") or key.endswith("_us"))
                and abs(item) > 2_147_483_647
            ):
                out[key] = str(item)
            else:
                out[key] = json_reader_safe(item)
        return out
    if isinstance(value, list):
        return [json_reader_safe(item) for item in value]
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract a CSF replay trace from raw Rocket/rippled logs."
    )
    parser.add_argument("case_dir", help="Rocket case directory, e.g. G53T17")
    parser.add_argument("--seq", type=int, default=DEFAULT_TARGET_SEQ)
    parser.add_argument("--tx", default=DEFAULT_TARGET_TX, help="Disputed transaction hash")
    parser.add_argument(
        "--has-target-txset",
        default=DEFAULT_HAS_TARGET_TXSET,
        help="TxSet hash that contains --tx in the binary CSF replay",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output JSON path. Defaults to CASE_DIR/bug5_csf_trace.json",
    )
    args = parser.parse_args(argv)

    case_dir = Path(args.case_dir)
    out = Path(args.out) if args.out else case_dir / "bug5_csf_trace.json"
    trace = json_reader_safe(build_trace(
        case_dir,
        target_seq=args.seq,
        target_tx=args.tx,
        has_target_txset=args.has_target_txset,
    ))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(trace, indent=2, sort_keys=True), encoding="utf-8")

    replay = trace["replay"]
    print(out)
    print(
        "summary: "
        f"nodes={len(replay['nodes'])}, "
        f"deliveries={len(replay['deliveries'])}, "
        f"ticks={len(replay['ticks'])}, "
        f"accept_ticks={len(replay['accept_ticks'])}, "
        f"expected_proposals={len(replay['expected_proposals'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
