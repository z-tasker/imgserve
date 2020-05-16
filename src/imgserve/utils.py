from __future__ import annotations


class InvalidSliceArgumentError(Exception):
    pass


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
    slice_index += 1 # zero indexed
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
