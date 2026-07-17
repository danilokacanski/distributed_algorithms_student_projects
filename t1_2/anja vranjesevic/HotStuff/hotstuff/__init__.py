"""HotStuff BFT consensus core (paper: Yin et al., PODC '19).

Application-agnostic: the replica logic knows only about Nodes, QCs and Msgs.
The bank ledger (see `bank/`) plugs in as the replicated state machine.
"""
