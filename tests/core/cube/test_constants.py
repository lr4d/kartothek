from kartothek.core.cube.constants import KLEE_UUID_SEPERATOR
from kartothek.core.dataset import _validate_uuid


def test_uuid_seperator_valid():
    assert _validate_uuid(KLEE_UUID_SEPERATOR)
