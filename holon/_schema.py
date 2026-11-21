from __future__ import annotations

from collections.abc import Iterator
from fractions import Fraction
from functools import cache
from typing import Any

import jsonschema
from jsonschema.protocols import Validator

_STOCK = jsonschema.Draft202012Validator.VALIDATORS["multipleOf"]


def _fraction(number: float) -> Fraction:
    return Fraction(repr(number) if isinstance(number, float) else number)


def _multiple_of(
    validator: Validator, divisor: float, instance: Any, schema: object
) -> Iterator[jsonschema.ValidationError]:
    if validator.is_type(instance, "number") and (
        _fraction(instance) % _fraction(divisor)
    ):
        yield jsonschema.ValidationError(f"{instance!r} is not a multiple of {divisor}")


@cache
def exact(cls: type[Validator]) -> type[Validator]:
    extended: type[Validator] = jsonschema.validators.extend(
        cls,
        {
            keyword: _multiple_of
            for keyword, check in cls.VALIDATORS.items()
            if check is _STOCK
        },
    )
    return extended
