# Money and Numeric Precision

How to model monetary amounts and other exact decimal quantities on events, and why
`float` is the wrong choice for them.

## The short version

Use `decimal.Decimal` for money on your event fields. It round-trips through the
event store exactly, with no configuration and no custom serializer.

```python
from decimal import Decimal

from eventsource import DomainEvent, register_event


@register_event
class OrderItemAdded(DomainEvent):
    aggregate_type: str = "Order"
    item_name: str
    price: Decimal
```

Construct it from a **string**, never from a float literal:

```python
OrderItemAdded(aggregate_id=order_id, item_name="Widget", price=Decimal("19.99"))
```

## Why not `float`

A `float` is a binary fraction. Decimal values like `0.1` and `19.99` have no exact
binary representation, so arithmetic on them accumulates error:

```pycon
>>> 0.1 + 0.2
0.30000000000000004
>>> total = 0.0
>>> for _ in range(10):
...     total += 0.1
>>> total
0.9999999999999999
>>> total == 1.0
False
```

For a ledger this is disqualifying. Ten dime deposits must equal one dollar, and a
sum of stored amounts must equal the total you reported to the customer. `Decimal`
gives you that:

```pycon
>>> from decimal import Decimal
>>> sum([Decimal("0.1")] * 10) == Decimal("1.0")
True
```

The failure is easy to miss in testing because a *single* value survives a round
trip fine — `float("19.99")` prints back as `19.99`. The error only appears once
you start adding, multiplying, or comparing, which is exactly what an aggregate
does when it folds events into state.

## How `Decimal` survives storage

Events serialize with Pydantic's `model_dump(mode="json")`, which renders a
`Decimal` as its **string** form. The string is what lands in the JSON payload, and
when the event is read back, the field's `Decimal` annotation parses it to the exact
same value:

```pycon
>>> event = OrderItemAdded(aggregate_id=order_id, item_name="Widget", price=Decimal("19.99"))
>>> event.to_dict()["price"]
'19.99'
>>> OrderItemAdded(**event.to_dict()).price
Decimal('19.99')
```

There is no float in that path, so there is no precision to lose. This holds
through a full store round-trip — append and read back and you get
`Decimal('19.99')`, not a string and not a float.

### The table that looks like it says otherwise

The [serialization README](https://github.com/tyevans/eventsource-py/blob/main/src/eventsource/adapters/serialization/README.md)
lists `decimal.Decimal` among the types its encoder does *not* handle, and calling
the raw encoder on one does raise:

```pycon
>>> from eventsource.adapters.serialization.json import json_dumps
>>> json_dumps({"price": Decimal("19.99")})
TypeError: Type is not JSON serializable: decimal.Decimal
```

That table describes `json_dumps` when you hand it a `Decimal` **directly**. Events
never do: Pydantic converts the `Decimal` to a string first, and `json_dumps` only
ever sees the string. Both facts are true and they are about different code paths.
If you call `json_dumps` yourself on a structure containing a raw `Decimal`, convert
it at the call site as that README describes.

## Typed fields rehydrate; `metadata` does not

This asymmetry catches people. A `Decimal` on a **declared field** comes back as a
`Decimal`, because the annotation tells Pydantic how to parse it. The same value in
the untyped `metadata` dict comes back as a **string**, because `metadata` is
`dict[str, Any]` and there is no annotation to parse against:

```pycon
>>> event = OrderItemAdded(..., price=Decimal("1.10"), metadata={"amt": Decimal("1.10")})
>>> restored = OrderItemAdded(**event.to_dict())
>>> restored.price
Decimal('1.10')
>>> restored.metadata["amt"]
'1.10'
```

If a number matters to your domain, give it a declared field. `metadata` is for
provenance and debugging, not for values you compute with.

## Rounding is your job

`Decimal` is exact, which means division produces as many digits as it needs:

```pycon
>>> Decimal("19.99") / 3
Decimal('6.663333333333333333333333333')
```

Nothing truncates that for you before it is stored. Quantize at the point where you
decide the value, so the number you persist is the number you meant:

```python
from decimal import Decimal, ROUND_HALF_UP

share = (total / 3).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
```

Pick the rounding mode deliberately — `ROUND_HALF_UP` matches most invoicing rules,
while Python's default `ROUND_HALF_EVEN` is a statistical convention that will
surprise an accountant.

## Read models have their own constraint

Storing the event is only half the trip. When a projection writes a `Decimal` into a
read model, the fidelity depends on the backend:

| Backend | Column | Fidelity |
| --- | --- | --- |
| PostgreSQL | `DECIMAL(18, 6)` | exact, to six decimal places |
| SQLite | `REAL` | **lossy** — the value becomes a float |
| In-memory | Python object | exact |

Two consequences. Amounts needing more than six decimal places (unit costs, FX
rates) do not fit `DECIMAL(18, 6)` as configured; declare the precision you need in
your own schema. And a monetary read model verified only against SQLite is not
verified — the float behavior from the top of this page reappears there. See
[Read Models](read-models.md) for the full type-fidelity comparison.

## When integer minor units are better

Storing an integer count of the smallest currency unit — cents, pence, satoshi — is
the other defensible choice:

```python
class OrderItemAdded(DomainEvent):
    aggregate_type: str = "Order"
    price_cents: int
```

It is exact by construction, serializes as a JSON number, needs no quantization
policy, and sidesteps the SQLite `REAL` problem entirely. The costs are that every
boundary must agree on the scale, currencies with other exponents (JPY has no minor
unit; several dinars have three digits) need care, and any division still forces a
rounding decision you now have to make in application code.

Prefer `Decimal` when amounts are read and written by people or external systems in
major units, and when the arithmetic is mostly addition. Prefer minor units when you
are doing high-volume integer accounting or already have a downstream contract in
cents. **Do not mix the two in one event stream** — the ambiguity is worse than
either choice.

## Rules of thumb

- Money on an event field: `Decimal`, constructed from a string.
- Never `float` for a value you will add up or compare for equality.
- Quantize before persisting; choose the rounding mode on purpose.
- Values you compute with belong in declared fields, not `metadata`.
- Check your read model's column type before trusting a monetary projection.
