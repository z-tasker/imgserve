from imgserve.utils import get_batch_slice


def test_get_batch_slice() -> None:
    explicit_145 = [ i for i in range(145) ]

    assert get_batch_slice(explicit_145, "1 of 10") == [ i for range(14) ]
    assert get_batch_slice(explicit_145, "10 of 10") == [ i for range(15) ]

