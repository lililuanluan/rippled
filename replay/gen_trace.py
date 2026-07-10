#!/usr/bin/env python3
"""Generate a deterministic CSF replay trace from raw Rocket/rippled logs.

This script is intentionally self-contained for bug reports: it only uses the
Python standard library, consumes the raw case directory, and writes the JSON
that replay/main.cpp consumes.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EMPTY_HASH = "0" * 64
RIPPLE_EPOCH_OFFSET_SECONDS = 946_684_800

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
DISPUTED_TX_RE = re.compile(r"Transaction ([A-F0-9]+) is disputed")
TIMER_RE = re.compile(r"ConsensusLogger Heartbeat Timer:")
CHECK_CONSENSUS_RE = re.compile(
    r"checkConsensus: prop=(\d+)/(\d+) agree=(\d+) validated=(\d+) time=(\d+)/(\d+)"
)
BUILDING_SET_RE = re.compile(r"Building canonical tx set: ([A-F0-9]+)")
BUILT_LEDGER_RE = re.compile(r"Built ledger #(\d+): ([A-F0-9]+)")
VALIDATION_ID_RE = re.compile(r"node_id: ([A-F0-9]{40}).*?master_key: (\S+)")
CLOSETIME_VALIDATION_ID_RE = re.compile(
    r"VALIDATION: .*?node_id: ([A-F0-9]+).*?base58: ([A-Za-z0-9]+)"
)
PROPOSAL_RE = re.compile(
    r"PROPOSAL proposal: previous_ledger: ([A-F0-9]+) "
    r"proposal_seq: (\d+) position: ([A-F0-9]+) "
    r"close_time: (.*?) now: .*? is_bow_out:(\d+) node_id: ([A-F0-9]{40})"
)
REPORT_TXSET_RE = re.compile(r"Report: Transaction Set = ([A-F0-9]+), close (\d+)")
CNF_VAL_RE = re.compile(r"CNF Val ([A-F0-9]+)")
OPEN_TRANSITION = "transitioned to ConsensusPhase::open"
ESTABLISH_TRANSITION = "transitioned to ConsensusPhase::establish"


@dataclass
class LogRow:
    idx: int
    line_no: int
    timestamp: str
    time_us: int
    line: str


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


def load_network(
    case_dir: Path, iteration_dir: Path, node_ids: list[int]
) -> tuple[list[int], list[dict[str, Any]]]:
    candidates = (case_dir / "network_input.yaml", iteration_dir.parent / "network_input.yaml")
    for path in candidates:
        if path.is_file():
            lines = path.read_text(encoding="utf-8").splitlines()

            def section(name: str) -> list[str]:
                start = next(
                    (i for i, line in enumerate(lines) if line == f"{name}:"), None
                )
                if start is None:
                    return []
                rows: list[str] = []
                for line in lines[start + 1 :]:
                    if line and not line[0].isspace() and not line.startswith("-"):
                        break
                    rows.append(line)
                return rows

            byzantine_nodes = [
                int(match.group(1))
                for line in section("byzz_nodes")
                if (match := re.fullmatch(r"\s*-\s*(\d+)\s*", line))
            ]

            unl_rows: list[list[int]] = []
            current: list[int] | None = None
            for line in section("unl_partition"):
                outer = re.fullmatch(r"-\s*-\s*(\d+)\s*", line)
                inner = re.fullmatch(r"\s+-\s*(\d+)\s*", line)
                if outer:
                    current = [int(outer.group(1))]
                    unl_rows.append(current)
                elif inner and current is not None:
                    current.append(int(inner.group(1)))

            if not unl_rows:
                unl_rows = [list(node_ids) for _ in node_ids]
            if len(unl_rows) != len(node_ids):
                raise ValueError(
                    f"unl_partition has {len(unl_rows)} rows, expected {len(node_ids)} in {path}"
                )
            unl = [
                {"node": node, "trusted": sorted(set(trusted))}
                for node, trusted in zip(node_ids, unl_rows, strict=True)
            ]
            return sorted(set(byzantine_nodes)), unl
    raise FileNotFoundError(f"no network_input.yaml found under {case_dir}")


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
                if match:
                    node = base58_to_node.get(match.group(2))
                    if node is not None:
                        hex_to_node[match.group(1)] = node
                        continue

                # Some test logs have `master_key: none` for an untrusted node
                # while still carrying the public validator key in `base58`.
                match = CLOSETIME_VALIDATION_ID_RE.search(line)
                if match:
                    node = base58_to_node.get(match.group(2))
                    if node is not None:
                        hex_to_node[match.group(1)] = node
    return hex_to_node


def read_timestamped_log(path: Path) -> list[LogRow]:
    rows: list[LogRow] = []
    with path.open(encoding="utf-8", errors="ignore") as f:
        for idx, line in enumerate(f):
            line = line.rstrip("\n")
            ts = timestamp(line)
            if ts is None:
                continue
            rows.append(
                LogRow(
                    idx=idx,
                    line_no=idx + 1,
                    timestamp=ts,
                    time_us=timestamp_us(ts),
                    line=line,
                )
            )
    return rows


def close_time_seconds(value: str) -> int:
    value = value.strip()
    if not value:
        return 0
    if value.isdigit():
        seconds = int(value)
        if seconds > RIPPLE_EPOCH_OFFSET_SECONDS:
            return seconds - RIPPLE_EPOCH_OFFSET_SECONDS
        return seconds
    if value.startswith("2000-Jan-01 00:00:00"):
        return 0
    return int(parse_timestamp(value).timestamp()) - RIPPLE_EPOCH_OFFSET_SECONDS


def collect_rounds(
    debug_logs: dict[int, Path],
) -> tuple[dict[int, list[dict[str, Any]]], dict[int, list[LogRow]], dict[str, int]]:
    logs = {node: read_timestamped_log(path) for node, path in sorted(debug_logs.items())}
    rounds_by_node: dict[int, list[dict[str, Any]]] = {}
    ledger_seq_by_hash: dict[str, int] = {}

    for node, rows in logs.items():
        starts: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            match = START_ROUND_RE.search(row.line)
            if not match:
                continue
            prev_seq = int(match.group(2))
            prev_ledger = match.group(1)
            ledger_seq_by_hash[prev_ledger] = prev_seq
            starts.append(
                {
                    "node": node,
                    "working_seq": prev_seq + 1,
                    "prev_seq": prev_seq,
                    "prev_ledger": prev_ledger,
                    "start_idx": i,
                    "start_timestamp": row.timestamp,
                    "start_us_abs": row.time_us,
                    "start_source": f"{debug_logs[node].name}:{row.line_no}",
                    "current_peer_proposals": int(match.group(3)),
                    "previous_proposers": int(match.group(4)),
                    "round_start_timestamp": row.timestamp,
                    "round_start_us_abs": row.time_us,
                    "round_start_source": f"{debug_logs[node].name}:{row.line_no}",
                    "establish_timestamp": row.timestamp,
                    "establish_us_abs": row.time_us,
                    "establish_source": f"{debug_logs[node].name}:{row.line_no}",
                    "prev_round_time_ms": None,
                    "end_us_abs": None,
                    "end_timestamp": None,
                }
            )

        for idx, round_info in enumerate(starts):
            next_idx = starts[idx + 1]["start_idx"] if idx + 1 < len(starts) else len(rows)
            next_us = starts[idx + 1]["start_us_abs"] if idx + 1 < len(starts) else None
            round_info["end_us_abs"] = next_us
            round_info["end_timestamp"] = (
                starts[idx + 1]["start_timestamp"] if idx + 1 < len(starts) else None
            )

            search_begin = max(0, int(round_info["start_idx"]) - 40)
            open_row: LogRow | None = None
            for candidate in rows[search_begin : int(round_info["start_idx"]) + 1]:
                if open_row is None and OPEN_TRANSITION in candidate.line:
                    open_row = candidate
            if open_row is not None:
                round_info["round_start_timestamp"] = open_row.timestamp
                round_info["round_start_us_abs"] = open_row.time_us
                round_info["round_start_source"] = f"{debug_logs[node].name}:{open_row.line_no}"

            establish_row: LogRow | None = None
            prev_round_ms: int | None = None
            for candidate in rows[search_begin:next_idx]:
                if establish_row is None and ESTABLISH_TRANSITION in candidate.line:
                    establish_row = candidate
                close_match = CLOSE_LEDGER_RE.search(candidate.line)
                if close_match and prev_round_ms is None:
                    prev_round_ms = int(close_match.group(1))
            if establish_row is not None:
                round_info["establish_timestamp"] = establish_row.timestamp
                round_info["establish_us_abs"] = establish_row.time_us
                round_info["establish_source"] = f"{debug_logs[node].name}:{establish_row.line_no}"
            round_info["prev_round_time_ms"] = prev_round_ms

        rounds_by_node[node] = starts

    return rounds_by_node, logs, ledger_seq_by_hash


def active_round(rounds: list[dict[str, Any]], time_us_abs: int) -> dict[str, Any] | None:
    candidates = [
        r
        for r in rounds
        if int(r["start_us_abs"]) <= time_us_abs
        and (r.get("end_us_abs") is None or time_us_abs < int(r["end_us_abs"]))
    ]
    return candidates[-1] if candidates else None


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


def collect_event_proposal_deliveries(
    debug_logs: dict[int, Path],
    *,
    ledger_seq_by_hash: dict[str, int],
    hex_to_node: dict[str, int],
) -> list[dict[str, Any]]:
    deliveries: list[dict[str, Any]] = []
    for receiver, path in sorted(debug_logs.items()):
        for row in read_timestamped_log(path):
            match = PROPOSAL_RE.search(row.line)
            if not match:
                continue

            prev_ledger = match.group(1)
            prev_seq = ledger_seq_by_hash.get(prev_ledger)
            if prev_seq is None:
                continue

            sender_hex = match.group(6)
            sender = hex_to_node.get(sender_hex)
            if sender is None:
                continue

            position = match.group(3)
            deliveries.append(
                {
                    "seq": prev_seq + 1,
                    "time_us_abs": row.time_us,
                    "sender": sender,
                    "receiver": receiver,
                    "proposal_seq": int(match.group(2)),
                    "position_hash": position,
                    "close_time": close_time_seconds(match.group(4)),
                }
            )
    deliveries.sort(
        key=lambda r: (
            int(r["time_us_abs"]),
            int(r["seq"]),
            int(r["receiver"]),
            int(r["sender"]),
            int(r["proposal_seq"]),
        )
    )
    return deliveries


def collect_event_txset_memberships(
    debug_logs: dict[int, Path],
    *,
    rounds_by_node: dict[int, list[dict[str, Any]]],
    hex_to_node: dict[str, int],
) -> list[dict[str, Any]]:
    memberships: dict[tuple[int, str, str], bool] = {}
    for receiver, path in sorted(debug_logs.items()):
        latest_position_by_peer: dict[str, str] = {}
        pending_tx: tuple[int, str] | None = None

        for row in read_timestamped_log(path):
            proposal_match = PROPOSAL_RE.search(row.line)
            if proposal_match:
                latest_position_by_peer[proposal_match.group(6)] = proposal_match.group(3)
                continue

            if CREATE_DISPUTES_RE.search(row.line):
                current = active_round(rounds_by_node[receiver], row.time_us)
                pending_tx = None if current is None else (int(current["working_seq"]), "")
                continue

            disputed_match = DISPUTED_TX_RE.search(row.line)
            if disputed_match and pending_tx is not None:
                pending_tx = (pending_tx[0], disputed_match.group(1))
                continue

            vote_match = PEER_VOTE_RE.search(row.line)
            if vote_match and pending_tx is not None and pending_tx[1]:
                peer_hex = vote_match.group(1)
                if peer_hex not in hex_to_node:
                    continue
                position_hash = latest_position_by_peer.get(peer_hex)
                if position_hash is None:
                    continue
                key = (pending_tx[0], position_hash, pending_tx[1])
                present = vote_match.group(2) == "YES"
                old = memberships.get(key)
                if old is not None and old != present:
                    raise ValueError(
                        "conflicting txset membership inferred for "
                        f"seq={pending_tx[0]} txset={position_hash} tx={pending_tx[1]}"
                    )
                memberships[key] = present
                continue

            if pending_tx is not None and "differences found" in row.line:
                pending_tx = None

    return [
        {
            "seq": seq,
            "txset_hash": txset_hash,
            "tx_hash": tx_hash,
            "present": present,
        }
        for (seq, txset_hash, tx_hash), present in sorted(memberships.items())
    ]


def collect_event_timer_ticks(
    logs: dict[int, list[LogRow]],
    rounds_by_node: dict[int, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    ticks: list[dict[str, Any]] = []
    for node, rows in sorted(logs.items()):
        for row in rows:
            if not TIMER_RE.search(row.line) or "Phase establish." not in row.line:
                continue
            if "updateOurPositions." not in row.line:
                continue
            current = active_round(rounds_by_node[node], row.time_us)
            if current is None:
                continue
            consensus_match = CHECK_CONSENSUS_RE.search(row.line)
            ticks.append(
                {
                    "seq": int(current["working_seq"]),
                    "time_us_abs": row.time_us,
                    "node": node,
                    "observed_validated": 0
                    if consensus_match is None
                    else int(consensus_match.group(4)),
                }
            )
    ticks.sort(key=lambda r: (int(r["time_us_abs"]), int(r["seq"]), int(r["node"])))
    return ticks


def collect_event_accepts(
    logs: dict[int, list[LogRow]],
    rounds_by_node: dict[int, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    accepts: list[dict[str, Any]] = []
    for node, rows in sorted(logs.items()):
        pending_txset_by_seq: dict[int, str] = {}
        pending_close_by_seq: dict[int, int] = {}
        for row in rows:
            current = active_round(rounds_by_node[node], row.time_us)
            if current is None:
                continue
            seq = int(current["working_seq"])

            report_match = REPORT_TXSET_RE.search(row.line)
            if report_match:
                pending_close_by_seq[seq] = int(report_match.group(2))
                continue

            build_match = BUILDING_SET_RE.search(row.line)
            if build_match:
                pending_txset_by_seq[seq] = build_match.group(1)
                continue

            built_match = BUILT_LEDGER_RE.search(row.line)
            if built_match:
                built_seq = int(built_match.group(1))
                accepts.append(
                    {
                        "seq": built_seq,
                        "time_us_abs": row.time_us,
                        "node": node,
                        "ledger_hash": built_match.group(2),
                        "txset_hash": pending_txset_by_seq.get(built_seq, ""),
                        "close_time": pending_close_by_seq.get(built_seq, 0),
                    }
                )
    accepts.sort(key=lambda r: (int(r["time_us_abs"]), int(r["seq"]), int(r["node"])))
    return accepts


def collect_event_validations(
    logs: dict[int, list[LogRow]],
    accepts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    ledger_to_seq = {
        str(row["ledger_hash"]): int(row["seq"])
        for row in accepts
        if row.get("ledger_hash")
    }
    validations: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str]] = set()
    for node, rows in sorted(logs.items()):
        for row in rows:
            match = CNF_VAL_RE.search(row.line)
            if not match:
                continue
            ledger_hash = match.group(1)
            seq = ledger_to_seq.get(ledger_hash)
            if seq is None:
                continue
            key = (node, seq, ledger_hash)
            if key in seen:
                continue
            seen.add(key)
            validations.append(
                {
                    "seq": seq,
                    "time_us_abs": row.time_us,
                    "node": node,
                    "ledger_hash": ledger_hash,
                }
            )
    validations.sort(key=lambda r: (int(r["time_us_abs"]), int(r["seq"]), int(r["node"])))
    return validations


def build_event_trace(case_dir: Path) -> dict[str, Any]:
    case_dir = case_dir.expanduser().resolve()
    iteration_dir = resolve_iteration_dir(case_dir)
    base58_to_node, nodes = load_node_info(iteration_dir)
    byzantine_nodes, unl = load_network(case_dir, iteration_dir, sorted(nodes))
    hex_to_node = load_hex_node_map(iteration_dir, base58_to_node)
    debug_logs = select_logs(iteration_dir, debug=True)

    rounds_by_node, logs, ledger_seq_by_hash = collect_rounds(debug_logs)
    deliveries = collect_event_proposal_deliveries(
        debug_logs, ledger_seq_by_hash=ledger_seq_by_hash, hex_to_node=hex_to_node
    )
    txset_memberships = collect_event_txset_memberships(
        debug_logs, rounds_by_node=rounds_by_node, hex_to_node=hex_to_node
    )
    ticks = collect_event_timer_ticks(logs, rounds_by_node)
    accepts = collect_event_accepts(logs, rounds_by_node)
    validations = collect_event_validations(logs, accepts)

    timestamp_values: list[int] = []
    for rounds in rounds_by_node.values():
        for row in rounds:
            timestamp_values.append(int(row["round_start_us_abs"]))
            timestamp_values.append(int(row["establish_us_abs"]))
    for rows in (deliveries, ticks, accepts, validations):
        timestamp_values.extend(int(row["time_us_abs"]) for row in rows)
    if not timestamp_values:
        raise ValueError(f"no replay events found in {case_dir}")
    base_us = min(timestamp_values)

    def rel_us(value: int) -> int:
        return value - base_us

    initial_position: dict[tuple[int, int], str] = {}
    initial_close: dict[tuple[int, int], int] = {}
    for row in deliveries:
        if int(row["proposal_seq"]) != 0:
            continue
        key = (int(row["seq"]), int(row["sender"]))
        initial_position.setdefault(key, str(row["position_hash"]))
        initial_close.setdefault(key, int(row["close_time"]))

    accept_by_node_seq = {
        (int(row["node"]), int(row["seq"])): row
        for row in accepts
    }
    accept_by_seq: dict[int, dict[str, Any]] = {}
    for row in accepts:
        accept_by_seq.setdefault(int(row["seq"]), row)

    rounds: list[dict[str, Any]] = []
    for node, node_rounds in sorted(rounds_by_node.items()):
        for row in node_rounds:
            seq = int(row["working_seq"])
            prev_accept = accept_by_node_seq.get((node, seq - 1)) or accept_by_seq.get(seq - 1)
            rounds.append(
                {
                    "seq": seq,
                    "node": node,
                    "round_start_us": rel_us(int(row["round_start_us_abs"])),
                    "establish_us": rel_us(int(row["establish_us_abs"])),
                    "prev_round_time_ms": int(row["prev_round_time_ms"] or 2000),
                    "prev_proposers": int(row["previous_proposers"]),
                    "initial_position_hash": initial_position.get((seq, node), EMPTY_HASH),
                    "initial_close_time": initial_close.get((seq, node), 0),
                    "prev_close_time": 0
                    if prev_accept is None
                    else int(prev_accept.get("close_time") or 0),
                }
            )
    rounds.sort(key=lambda r: (int(r["seq"]), int(r["node"])))

    def strip_time_abs(row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        out["at_us"] = rel_us(int(out.pop("time_us_abs")))
        return out

    accept_rows = [
        {
            "seq": int(row["seq"]),
            "node": int(row["node"]),
            "ledger_hash": str(row["ledger_hash"]),
            "txset_hash": str(row["txset_hash"]),
        }
        for row in accepts
    ]
    validation_rows = [
        {
            "seq": int(row["seq"]),
            "node": int(row["node"]),
            "ledger_hash": str(row["ledger_hash"]),
        }
        for row in validations
    ]
    return {
        "schema": "rocket.csf_replay_events.v1",
        "byzantine_nodes": byzantine_nodes,
        "unl": unl,
        "rounds": rounds,
        "proposal_deliveries": [strip_time_abs(row) for row in deliveries],
        "txset_memberships": txset_memberships,
        "timer_ticks": [strip_time_abs(row) for row in ticks],
        "accepts": accept_rows,
        "validations": validation_rows,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract a CSF replay trace from raw Rocket/rippled logs."
    )
    parser.add_argument("case_dir", type=Path, help="Rocket case directory, e.g. G53T17")
    parser.add_argument(
        "out",
        nargs="?",
        type=Path,
        default=Path("trace.json"),
        help="Output JSON path. Defaults to ./trace.json",
    )
    args = parser.parse_args(argv)

    case_dir = args.case_dir.expanduser().resolve()
    out = args.out.expanduser()
    trace = json_reader_safe(build_event_trace(case_dir))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(trace, indent=2, sort_keys=True), encoding="utf-8")

    summary = (
        f"wrote {out}: schema={trace['schema']}, "
        f"rounds={len(trace['rounds'])}, deliveries={len(trace['proposal_deliveries'])}, "
        f"memberships={len(trace['txset_memberships'])}, "
        f"ticks={len(trace['timer_ticks'])}, accepts={len(trace['accepts'])}, "
        f"validations={len(trace['validations'])}"
    )
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
