"""The finding has to survive a change of seed, or it is not a finding.

The results page reports that one store's delivery promise is more than twice as
wrong as the other's, and that a pooled average hides it. Those numbers come
from generated data, so the obvious objection is the right one: is this a
property of the system, or of the particular random draw that produced it?

The generator holds both kinds of thing. The STRUCTURE is fixed -- NORTHGATE moves
at 17 km/h and RIVERSIDE at 25, written into the source, while the promise formula
uses one global speed for both. The NOISE is seeded -- which store each trip
belongs to, how far it goes, how long each leg takes.

So the claim being tested is that the finding comes from the structure. Change
the seed and the decimals move; change the seed and the ordering must not.

Without this, somebody edits the two speeds closer together, the finding quietly
evaporates, every other test still passes, and the results page goes on
asserting it.
"""

from __future__ import annotations

import argparse
import statistics
from datetime import datetime

import pytest

from dispatch.contracts import ContractViolation, normalise_event
from dispatch.dedupe import dedupe
from dispatch.sessionize import sessionize
from generator.produce import STORES, generate

AS_OF = datetime(2026, 9, 16)
SEEDS = (42, 7, 1234, 99999)


def gen_args(seed: int, trips: int = 1500):
    """Smaller than a demo run, same shape. The property under test is an
    ordering, and an ordering does not need 20,000 trips to show up."""
    return argparse.Namespace(
        trips=trips, days=14, seed=seed, dup_rate=0.04, correction_rate=0.01,
        late_rate=0.02, v2_rate=0.25, null_driver_rate=0.01, broken_rate=0.005,
        cancel_rate=0.03, v110_rate=0.15, rogue_field_rate=0.02)


def promise_error_by_store(seed: int, trips: int = 1500) -> dict:
    """Mean signed promise error per store, plus the pooled figure.

    Computed in Python rather than through the warehouse: the SQL marts are
    tested elsewhere, and what matters here is the arithmetic the page reports,
    not the path it took to get there.
    """
    events = []
    for raw in generate(gen_args(seed, trips)):
        try:
            events.append(normalise_event(raw, ingested_at=AS_OF))
        except ContractViolation:
            continue

    facts = sessionize(dedupe(events).rows, as_of=AS_OF)
    errors: dict[str, list[float]] = {}
    pooled: list[float] = []
    for fact in facts:
        if not fact.measurable or fact.promised_minutes is None or not fact.store_id:
            continue
        error = fact.tat_minutes - fact.promised_minutes
        errors.setdefault(fact.store_id, []).append(error)
        pooled.append(error)

    out = {store: statistics.fmean(values) for store, values in errors.items()}
    out["_pooled"] = statistics.fmean(pooled)
    out["_n"] = len(pooled)
    return out


@pytest.fixture(scope="module")
def across_seeds():
    """One pipeline pass per seed, shared by the tests below.

    Module-scoped because generating and sessionising four times is the
    expensive part, and every test here asks a different question of the same
    four answers.
    """
    return {seed: promise_error_by_store(seed) for seed in SEEDS}


def test_the_slower_store_is_always_the_more_wrong_one(across_seeds):
    """The finding itself. NORTHGATE moves at 17 km/h against RIVERSIDE's 25, and the
    promise formula knows about neither, so NORTHGATE must come out worse on every
    draw. If this fails, either the speeds were changed or the formula started
    reading the store."""
    for seed, result in across_seeds.items():
        assert result["NORTHGATE"] > result["RIVERSIDE"], (
            f"seed {seed}: NORTHGATE {result['NORTHGATE']:.2f} is not worse than "
            f"RIVERSIDE {result['RIVERSIDE']:.2f} -- the finding did not survive")


def test_the_gap_is_large_enough_to_be_worth_reporting(across_seeds):
    """'More than twice as wrong' is the claim on the results page. A gap of a
    few tenths would be noise wearing a finding's clothes."""
    for seed, result in across_seeds.items():
        ratio = result["NORTHGATE"] / result["RIVERSIDE"]
        assert ratio > 1.8, (
            f"seed {seed}: ratio {ratio:.2f} is too small for the page's claim")


def test_the_pooled_average_sits_between_the_two_stores(across_seeds):
    """Why a pooled figure hides the problem, stated as an assertion.

    Lying strictly between the two is what makes one number look like one
    moderate problem instead of two different ones pointing opposite ways.
    """
    for seed, result in across_seeds.items():
        assert result["RIVERSIDE"] < result["_pooled"] < result["NORTHGATE"], (
            f"seed {seed}: pooled {result['_pooled']:.2f} is not between "
            f"{result['RIVERSIDE']:.2f} and {result['NORTHGATE']:.2f}")


def test_the_decimals_move_but_the_ordering_does_not(across_seeds):
    """The honest version of the claim, and the reason this file exists.

    The exact figures are a property of the draw and must not be quoted as
    though they were a measurement of the world. The ordering is a property of
    the system. This asserts both halves: that the numbers genuinely vary, and
    that the conclusion genuinely does not.
    """
    fc002 = [r["NORTHGATE"] for r in across_seeds.values()]
    fc004 = [r["RIVERSIDE"] for r in across_seeds.values()]

    # They move -- if they did not, the seed is not reaching the generator and
    # every other test here would be checking the same run four times.
    assert len({round(v, 2) for v in fc002}) > 1, "NORTHGATE identical across seeds"
    assert max(fc002) - min(fc002) > 0.01

    # And they never cross.
    assert min(fc002) > max(fc004), (
        f"the worst NORTHGATE draw ({min(fc002):.2f}) must still beat the best "
        f"RIVERSIDE draw ({max(fc004):.2f}) or the ordering is luck")


def test_the_same_seed_gives_the_same_answer_twice():
    """Reproducibility, which is what makes any number on the page quotable.

    A figure that changes between two runs of the same command cannot go on a
    page, in a README, or in a conversation.
    """
    first = promise_error_by_store(42, trips=800)
    second = promise_error_by_store(42, trips=800)
    assert first == second


def test_the_generator_still_declares_two_differently_paced_stores():
    """Guards the premise rather than the conclusion.

    Every test above would also pass with a single store, vacuously or by
    KeyError-free accident. This pins the arrangement the finding depends on,
    so a change to it fails here -- next to an explanation -- rather than
    somewhere downstream.
    """
    speeds = {code: kmh for code, _share, kmh in STORES}
    assert len(speeds) >= 2, "the finding needs at least two stores"
    assert speeds["NORTHGATE"] < speeds["RIVERSIDE"], (
        "NORTHGATE is supposed to be the slower store; the finding is written "
        "around that and the results page says so")
    assert speeds["RIVERSIDE"] / speeds["NORTHGATE"] > 1.2, (
        "the two speeds are too close for the promise formula's single global "
        "speed to be visibly wrong at one of them")
