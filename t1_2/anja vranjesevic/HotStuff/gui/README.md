# GUI — BFT Consensus Control Room

A visual player for real HotStuff runs: the 4-replica message bus, the block
tree, per-replica state (view / lockedQC / prepareQC), the settlement ledger, and
a scrollable event log — all driven by a timeline you can play, pause, step, and
scrub.

## Files

| File | What it is |
|------|-----------|
| `index.html` | The GUI. Single self-contained file (inline CSS/JS, no CDNs). |
| `runs.js` | **Generated** — real recorded protocol runs, one per scenario, in the GUI event schema. Produced by `python -m tools.gen_gui_runs`. |

`index.html` loads `runs.js` when present and shows genuine data; without it, it
falls back to small baked-in sample arrays.

## Run it

```bash
python -m tools.gen_gui_runs      # (re)record the runs -> gui/runs.js
```

Then open `gui/index.html` in a browser (double-click works — no server needed),
or serve the folder:

```bash
python -m http.server -d gui 8000     # then visit http://localhost:8000
```

Pick a scenario top-left (Double-spend, Happy path, Leader crash, Byzantine
leader, Chained), press **play**, and adjust speed / step through events. The
runs are deliberately paced slowly so each phase is easy to follow.

## How the data gets here

The simulator's single log call, `hotstuff/log.py::event(...)`, forwards every
protocol event to `hotstuff/eventlog.py`, which captures it as a structured
record matching the schema this GUI consumes. `tools/gen_gui_runs.py` runs each
scenario with slowed timing, cleans up the stream (collapses the per-replica
duplicate DECIDEs, stretches the timeline), and writes `runs.js`. One
instrumentation path feeds both the console log and the GUI.
