# HotStuff — Interbank Settlement Ledger

A simulator of the **HotStuff** BFT consensus protocol (Yin, Malkhi, Reiter,
Golan Gueta, Abraham — *PODC '19*), applied to a real problem: an **interbank
settlement ledger** where four mutually distrusting banks keep a replicated,
totally-ordered transaction log with **no trusted central operator**.

Project for the course *Distributed Algorithms*. It implements both protocols
from the paper:

- **Basic HotStuff** (Algorithm 2) — the core deliverable.
- **Chained HotStuff** (Algorithm 3) — the pipelined variant, as a bonus.

The one idea the whole project is built to demonstrate:

> **Consensus orders the commands; the state machine judges them.**
> HotStuff decides *the order* of transfers. Whether a transfer succeeds is then
> a deterministic function of that order, evaluated identically on every replica —
> which is why replicas can never disagree, even about a rejected transfer.

## Requirements & running

```bash
pip install -r requirements.txt      # PyNaCl (Ed25519), PyYAML
```

Run the five demos (each prints a readable, story-like event log and ends in a
`PASS`):

```bash
python -m demos.demo1_happy_path       # Basic HotStuff, 4 phases, happy path
python -m demos.demo2_leader_crash     # leader crash → rotation, view change, catch-up
python -m demos.demo3_double_spend     # double-spend resolved by consensus ordering
python -m demos.demo4_byzantine_leader # equivocating + censoring Byzantine leaders
python -m demos.demo5_chained          # Chained HotStuff + throughput comparison
```

Run the test suite (unit tests for crypto/tree/safeNode + end-to-end consensus,
view-change, ledger, Byzantine-safety and chained tests):

```bash
python -m pytest -q
```

Every knob (network delay/jitter/drop, per-view timeout, which replica is
Byzantine, fixed vs rotating leader, random seed) is a plain constructor argument
on `demos/harness.py::Cluster` — easy to change live during a defense.

## Visual GUI (optional)

A web "control room" replays real recorded runs — the message bus, block tree,
per-replica state, ledger balances and event log — on a play/pause/step timeline:

```bash
python -m tools.gen_gui_runs      # record the runs -> gui/runs.js
```

Then open `gui/index.html` (no server needed). See [gui/README.md](gui/README.md).
The GUI consumes the same event stream as the console log: `hotstuff/log.py`
forwards every `event(...)` to `hotstuff/eventlog.py`, which records it in the
GUI's schema — one instrumentation path, two views.

## What each demo shows

| Demo | Scenario | What it proves |
|------|----------|----------------|
| 1 | 20 commands, fixed leader, perfect net | The four phases commit; all 4 replicas' logs are byte-identical. |
| 2 | Kill a leader mid-view, later restart it | Rotation + timeout keep the other 3 live; the restarted replica catches up to an identical log. |
| 3 | `A=100`, concurrent `A→B 80` and `A→C 80` | Exactly one succeeds; all replicas agree which — winner varies by seed, agreement never does (50-run sweep). |
| 4 | Equivocating leader; censoring leader | Equivocation gathers no quorum (Lemma 1) → times out; censorship is defeated by rotation. Safety checker green. |
| 5 | Double-spend on Chained + throughput | Chained is a drop-in replacement (same ledger result) and out-commits Basic ~3× via pipelining. |

## Architecture

Four replicas run as **independent `asyncio` tasks** in one process (never a
sequential loop over nodes), connected by a simulated message bus. No sockets, no
serialization frameworks — the focus is the protocol.

```
hotstuff/                 # consensus core (application-agnostic)
  types.py       # Node, QC, Msg + canonical hashing            (paper §4.2)
  crypto.py      # Ed25519 sign/verify, QC build/verify         (paper §3)
  tree.py        # block tree, extends() / conflicts()          (paper §4.2)
  network.py     # simulated bus: send/broadcast, delay/drop/partition
  pacemaker.py   # leader(view)=view%4, timeouts, back-off      (paper §4.4)
  replica.py     # BASIC HotStuff, Algorithm 2 (+ view change, catch-up)
  chained_replica.py  # CHAINED HotStuff, Algorithm 3
  client.py      # submits commands, accepts on f+1 matching replies
  log.py         # one-line structured event log
bank/
  ledger.py      # accounts + deterministic apply(TRANSFER)  — the state machine
byzantine.py     # EquivocatingReplica, CensoringReplica (the only subclasses)
demos/
  harness.py     # Cluster: wires it together; convergence + safety checkers
  demo1..demo5   # the scenarios above
tests/           # pytest: crypto, tree, safeNode, consensus, view-change,
                 # ledger, safety-invariant, chained
```

**Code reads against the paper.** Phase handlers are named exactly as in the
paper (`on_prepare`, `on_pre_commit`, `on_commit`, `on_decide`, `on_new_view`,
`safe_node`, `create_leaf`), replica state uses the paper's names (`view_number`,
`locked_qc`, `prepare_qc`, `generic_qc`), and each handler's docstring cites the
Algorithm and line numbers it implements. The safety-critical **lock** is in
`replica.py::on_commit` (Algorithm 2, line 25), commented loudly.

### The safety-invariant checker

After a run, `Cluster.check_safety_invariants()` asserts the two properties from
the paper's proof directly against the observed run:

- **Theorem 2** — no two correct replicas ever commit conflicting branches
  (every pair of committed logs is prefix-consistent).
- **Lemma 1** — within one `(phase, view)` there is at most one QC (all QCs any
  correct replica observed for that phase+view name the same node). The
  equivocation demo is this lemma's living proof.

## Deviations from the paper (all safety-preserving)

1. **Signatures.** The paper aggregates one *threshold* signature per QC
   (`tcombine`). We instead keep an explicit list of `n − f = 3` distinct
   per-replica **Ed25519** signatures (PyNaCl). Safety is identical — a QC still
   proves a quorum signed the same `⟨type, view, node⟩` triple — only the
   authenticator is `O(n)` bytes instead of `O(1)`. See `hotstuff/crypto.py`.
2. **Fixed `n = 4, f = 1, quorum = 3`.** The paper's canonical smallest `3f+1`
   deployment; hard-coded for less code.
3. **In-process network.** Replicas are `asyncio` tasks and messages are passed
   by reference with a simulated delay/drop, instead of real TCP + serialization.
4. **`Node` carries `view_number`** (and, for Chained, its `justify` QC). A small
   convenience that keeps the tree unambiguous; excluded from the node hash.
5. **Catch-up** (`SYNC_REQ`/`SYNC_RESP`) is a ~30-line ancestor-fetch, not a
   full state-transfer subprotocol (§4.2 only notes that a replica may fetch
   missing ancestors from peers).

