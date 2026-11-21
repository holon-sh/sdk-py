from __future__ import annotations

import math
from collections.abc import Iterator
from fractions import Fraction
from functools import cache
from typing import Any

import jsonschema
from jsonschema.protocols import Validator
from referencing import Registry

_STOCK = jsonschema.Draft202012Validator.VALIDATORS["multipleOf"]


def _fraction(number: float) -> Fraction:
    return Fraction(repr(number) if isinstance(number, float) else number)


def _multiple_of(
    validator: Validator, divisor: float, instance: Any, schema: object
) -> Iterator[jsonschema.ValidationError]:
    if validator.is_type(instance, "number") and (
        (isinstance(instance, float) and not math.isfinite(instance))
        or _fraction(instance) % _fraction(divisor)
    ):
        yield jsonschema.ValidationError(f"{instance!r} is not a multiple of {divisor}")


@cache
def _exact(cls: type[Validator]) -> type[Validator]:
    extended: type[Validator] = jsonschema.validators.extend(
        cls,
        {
            keyword: _multiple_of
            for keyword, check in cls.VALIDATORS.items()
            if check is _STOCK
        },
    )
    return extended


def exact(
    schema: dict[str, Any], format_checker: jsonschema.FormatChecker | None = None
) -> Validator:
    cls = _exact(jsonschema.validators.validator_for(schema))
    cls.check_schema(schema)
    return cls(
        {key: value for key, value in schema.items() if key != "$schema"},
        registry=Registry(),
        format_checker=format_checker,
    )
