# NBFT — a highly fault-tolerant consensus simulator

Simulator of the NBFT algorithm from the paper:

> J. Yang, Z. Jia, R. Su, X. Wu, J. Qin, **"Improved Fault-Tolerant Consensus
Based on the PBFT Algorithm"**, IEEE Access, vol. 10, 2022.
> DOI: [10.1109/ACCESS.2022.3153701](https://doi.org/10.1109/ACCESS.2022.3153701)

NBFT splits the nodes into groups using **consistent hashing** (identities and
roles are unknown in advance), consensus is reached **first inside each group
and then among the group representatives**, and two defense models — the
**node decision broadcast model** and the **threshold vote-counting model** —
push the fault-tolerance upper bound **beyond 1/3** of the nodes (the
`[R, T]` interval), with a communication complexity of `O(⌊(n−1)/m⌋²)`
instead of PBFT's `O(n²)`.

## Installation

Python **3.9+** is required.

```bash
pip install -r requirements.txt
```

On Python 3.9 / 3.10 the `tomli` TOML backport is installed automatically
from `requirements.txt` (Python 3.11+ uses the standard-library `tomllib`
and needs no extra dependency).

## Running

List the available scenarios:

```bash
python main.py --list
```

Run a scenario:

```bash
python main.py --scenario happy_path
python main.py --scenario leader_timeout
python main.py --scenario low_signatures --verbose   # log every single message
```

Ad-hoc configuration without a preset (all flags can be combined):

```bash
python main.py -n 21 -m 4 --byzantine 3 --behavior equivocate --seed 7
python main.py -n 17 -m 4 --byzantine 2 --behavior crash --target representative
```

The process exit code is `0` when consensus succeeds and `1` when it does
not — convenient for scripting.

## Fault scenarios

| Scenario               | What it demonstrates                                                                                                                 |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `happy_path`           | two fault-free rounds; the primary rotates because it depends on the previous block hash                                             |
| `node_crash`           | ordinary members crash — their groups still gather`2E+1` signatures                                                                 |
| `leader_timeout`       | silent primary → client alert → voting → view change (the hash may re-elect a faulty node, so the network walks to the next view) |
| `message_delay`        | variable latency + 5% message loss                                                                                                   |
| `conflicting_messages` | byzantine nodes send different messages to different peers — both models cut their influence down                                   |
| `low_signatures`       | a representative forwards <`2E+1` signatures → its own group blocks it (Model 1) and votes on its own                               |

## Parameters (TOML preset or CLI)

| Parameter                     | CLI                     | Meaning                                                                     |
| ----------------------------- | ----------------------- | --------------------------------------------------------------------------- |
| `n`                           | `-n`                    | total number of nodes                                                       |
| `m`                           | `-m`                    | group size,`m = 3f₁ + 1` (4, 7, 10, …); `R = ⌊(n−1)/m⌋ ≥ 4` must hold |
| `rounds`                      | `--rounds`              | number of client requests                                                   |
| `seed`                        | `--seed`                | random seed (reproducible demonstrations)                                   |
| `byz_count`                   | `--byzantine`           | number of byzantine nodes                                                   |
| `byz_behavior`                | `--behavior`            | `crash`, `silent_leader`, `equivocate`, `low_sig`, `delay`                  |
| `byz_target`                  | `--target`              | `random`, `primary`, `representative`, `member`                             |
| `base_delay_ms` / `jitter_ms` | `--delay` / `--jitter`  | network delay                                                               |
| `loss_rate`                   | `--loss`                | message loss probability`[0..1]`                                            |
| `phase_timeout_ms`            | `--phase-timeout`       | phase timeout (Model 1, condition 2)                                        |
| `client_timeout_ms`           | `--client-timeout`      | how long the client waits for the reply quorum                              |
| `trace_level`                 | `--trace` / `-v` / `-q` | `quiet`, `normal`, `verbose`                                                |

At the end of every run the simulator prints a per-phase traffic table and
compares the measured message count against Formula 4 from the paper
(`C = 2(n−1) + 2(m−1)R + R²`) — for `n=17, m=4` a round costs exactly
**72 messages**, while PBFT would spend ~544.

## Experiments (charts)

```bash
python -m experiments.fnd_success          # Figures 4 and 5 of the paper (FND model, 200 trials per point)
python -m experiments.complexity           # Figure 6 of the paper (NBFT/PBFT traffic ratio)
python -m experiments.simulated_success    # the same curve measured with the full protocol (n=17)
```

The charts are written to `experiments/charts/`.

## Tests

```bash
python -m pytest
```

They cover the parameter formulas, hash-ring determinism, the threshold
vote-counting model and end-to-end simulations (happy path, crashes, blocked
representatives, view change, lossy network, consensus collapse beyond the
bound).

## Project layout

```
main.py                  CLI entry point
nbft/
  params.py              consensus parameters: R, E, w, T, the [R, T] interval
  hashring.py            consistent hashing: groups, primary, representatives
  messages.py            message types + simulated signatures
  network.py             asyncio network: delay, loss, traffic counters
  node.py                the node: all 7 phases, Model 1, view change
  voting.py              Model 2: threshold vote-counting
  byzantine.py           byzantine behaviors
  client.py              client: request + reply quorum (n−1)/2 + 1
  simulator.py           simulation orchestration
  trace.py               structured terminal output (rich)
scenarios/               TOML presets for the six scenarios
experiments/             FND experiment, complexity, measured success rate
tests/                   pytest suite
docs/                    project documentation 
```


