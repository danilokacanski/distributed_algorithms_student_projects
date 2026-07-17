"""M0 — sign/verify round-trip and QC build/verify rejection cases."""
from hotstuff.crypto import build_qc, gen_keys, sign_vote, verify_qc, verify_vote
from hotstuff.types import QC, Msg, MsgType, Node

N, F, QUORUM = 4, 1, 3
NODE = Node(parent_hash=None, cmd={"x": 1}, view_number=5)


def _vote(rid, sk, node=NODE, type=MsgType.PREPARE, view=5):
    return Msg(type=type, view_number=view, sender=rid, node=node,
               partial_sig=(rid, sign_vote(sk, type, view, node.hash)))


def test_sign_verify_roundtrip():
    sk, vk = gen_keys(N)
    sig = sign_vote(sk[2], MsgType.PREPARE, 5, NODE.hash)
    assert verify_vote(vk[2], MsgType.PREPARE, 5, NODE.hash, sig)


def test_verify_rejects_wrong_key():
    sk, vk = gen_keys(N)
    sig = sign_vote(sk[2], MsgType.PREPARE, 5, NODE.hash)
    assert not verify_vote(vk[3], MsgType.PREPARE, 5, NODE.hash, sig)


def test_verify_rejects_tampered_triple():
    sk, vk = gen_keys(N)
    sig = sign_vote(sk[0], MsgType.PREPARE, 5, NODE.hash)
    assert not verify_vote(vk[0], MsgType.COMMIT, 5, NODE.hash, sig)   # wrong type
    assert not verify_vote(vk[0], MsgType.PREPARE, 6, NODE.hash, sig)  # wrong view
    assert not verify_vote(vk[0], MsgType.PREPARE, 5, "deadbeef", sig)  # wrong node


def test_build_and_verify_valid_qc():
    sk, vk = gen_keys(N)
    votes = [_vote(i, sk[i]) for i in range(3)]
    qc = build_qc(votes, QUORUM)
    assert qc.signers() == {0, 1, 2}
    assert verify_qc(qc, vk, QUORUM)


def test_qc_rejected_too_few_signers():
    sk, vk = gen_keys(N)
    qc = build_qc([_vote(0, sk[0]), _vote(1, sk[1])], quorum=2)
    assert not verify_qc(qc, vk, QUORUM)  # only 2 signers, need 3


def test_qc_rejected_forged_signature():
    sk, vk = gen_keys(N)
    votes = [_vote(i, sk[i]) for i in range(3)]
    qc = build_qc(votes, QUORUM)
    forged = QC(qc.type, qc.view_number, qc.node_hash,
                sigs=qc.sigs[:2] + ((2, b"\x00" * 64),))
    assert not verify_qc(forged, vk, QUORUM)


def test_qc_rejected_duplicate_signer():
    sk, vk = gen_keys(N)
    v0 = _vote(0, sk[0])
    qc = QC(MsgType.PREPARE, 5, NODE.hash,
            sigs=(v0.partial_sig, v0.partial_sig, _vote(1, sk[1]).partial_sig))
    assert not verify_qc(qc, vk, QUORUM)  # signer 0 counted twice → < 3 distinct


def test_build_qc_dedups_and_takes_quorum():
    sk, vk = gen_keys(N)
    votes = [_vote(i, sk[i]) for i in range(4)]  # all 4 vote; QC keeps 3
    qc = build_qc(votes, QUORUM)
    assert len(qc.sigs) == QUORUM
    assert verify_qc(qc, vk, QUORUM)
