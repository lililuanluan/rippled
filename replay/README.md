# CSF Replay



## Usage

Run from the `replay/` directory:

```bash
./run.sh            # Run both replays
./run.sh txset      # Run only the transaction-set replay
./run.sh closetime  # Run only the close-time replay
```

The script generates the selected trace, configures and builds its target, and
then runs the replay.

## Changes Outside `replay/`

| file | code | why |
| --- | --- | --- |
| `CMakeLists.txt` | Adds standalone `txset` and `closetime` targets. | Builds the replay tools without building `rippled` or the full test suite. |
| `src/test/csf/Scheduler.h` | Orders events with identical timestamps by insertion order. | Keeps replay execution deterministic. |
| `src/test/csf/Peer.h` | Adds one-shot timer, previous-round/close controls, and a `proposersFinished` override. | Drives CSF with the timing and observed validation counts recorded in the trace. |
| `src/xrpld/consensus/Consensus.h` | Adds `TRACE_TEST` hooks for previous-round state and manual ledger close. | Exposes the minimum test controls while still running the original consensus logic. |
