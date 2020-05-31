from __future__ import annotations
from copy import copy

from .errors import InvalidSliceArgumentError


def get_batch_slice(items: List[Any], batch_slice: str) -> List[Any]:
    if " of " not in batch_slice and "/" not in batch_slice:
        raise InvalidSliceArgumentError(
            f"use ' of ' or ' / ' or '/' to separate batch_slice numerator and denominator"
        )
    try:
        slice_index, total_slices = [int(num) for num in batch_slice.split(" of ")]
    except:
        raise InvalidSliceArgumentError(
            f"could not parse {batch_slice} as a batch slice"
        )
    slice_index += 1  # zero indexed
    if total_slices == 0 or slice_index == 0:
        raise InvalidSliceArgumentError(
            f"numerator and denominator of batch_slice must be > 0"
        )
    slice_size = int(len(items) / total_slices)
    if slice_size < 1:
        raise InvalidSliceArgumentError(
            f"cannot break items of length {len(items)} into {total_slices} slices."
        )
    slices = list()
    for i in range(0, len(items), slice_size):
        slices.append(items[i : i + slice_size])
    # add the remainder on to the last slice
    len_of_slices = sum(map(len, slices))
    if len_of_slices < len(items):
        slices[total_slices].extend(items[len_of_slices:])

    return slices[slice_index - 1]


class AsteriskNotAtListError(KeyError):
    pass


class InvalidSplatError(KeyError):
    pass


class RecursedToKeyError(KeyError):
    pass


def recurse_splat_key(
    data: Dict[str, Any], value_keys: List[str]
) -> Generator[Any, None, None]:
    value_keys = copy(value_keys)
    """
        recurse to key with splat syntax for specifying "each key in list of objects"
    """
    try:
        if value_keys[-1] == "*":
            raise InvalidSplatError(f"cannot end splat with '*': {value_keys}")
    except IndexError as e:
        raise IndexError(f"recursed to empty keys: {data}")

    value_key = value_keys[0]

    if value_key == "*":
        if not isinstance(data, list):
            raise AsteriskNotAtListError(f"data is not a list, has keys {data.keys()}")
        for datum in data:
            yield from recurse_splat_key(datum, value_keys[1:])

    else:
        if value_key not in data:
            raise RecursedToKeyError(f"{data} contains no key {value_key}")

        data = data[value_key]

        value_keys.pop(0)
        if len(value_keys) == 0:
            yield data
        else:
            yield from recurse_splat_key(data, value_keys)
