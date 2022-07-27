from chia.types.blockchain_format.sized_bytes import bytes32
from chia.wallet.puzzles.load_clvm import load_clvm

CAT_MOD = load_clvm("cat.clvm", package_or_requirement=__name__)
LOCK_INNER_PUZZLE = load_clvm("lock.inner.puzzle.clvm", package_or_requirement=__name__)

CAT_MOD_HASH = CAT_MOD.get_tree_hash()
# WARNING: CAT1, Is subject to a security vulnerability, & is EOL, dont use it.
CAT1_MOD_HASH = bytes32.from_hexstr('72dec062874cd4d3aab892a0906688a1ae412b0109982e1797a170add88bdcdc')
