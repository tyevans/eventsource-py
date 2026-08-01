"""
Barrel-surface parity test for ``eventsource.application.subscriptions``
(ADR 0032 slice 2).

Confirms every name listed in ``__all__`` is actually importable as an
attribute of the package -- i.e. the ``__all__`` declaration and the actual
export surface haven't drifted apart during the package move.
"""

import eventsource.application.subscriptions as subscriptions_module


def test_all_names_in_dunder_all_are_importable_attributes() -> None:
    missing = [
        name for name in subscriptions_module.__all__ if not hasattr(subscriptions_module, name)
    ]
    assert not missing, f"__all__ names missing from module: {missing}"


def test_dunder_all_has_no_duplicates() -> None:
    names = subscriptions_module.__all__
    assert len(names) == len(set(names))
