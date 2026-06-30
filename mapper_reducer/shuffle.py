"""
shuffle.py

This module contains the shuffle-and-group stage for the local,
MapReduce-inspired text analytics pipeline.

The shuffle stage groups all identical keys together before the reducer
adds their values.
"""

from __future__ import annotations

from collections import defaultdict


def shuffle_and_group(
    mapped_pairs: list[tuple[str, int]],
) -> dict[str, list[int]]:
    """
    Group mapper output by word.

    Example input:
    [
        ("service", 1),
        ("guest", 1),
        ("service", 1),
    ]

    Example output:
    {
        "service": [1, 1],
        "guest": [1],
    }
    """
    grouped_pairs: dict[str, list[int]] = defaultdict(list)

    for word, count in mapped_pairs:
        grouped_pairs[word].append(count)

    # Convert defaultdict into a normal dictionary so the output is
    # easier to inspect, test, export, and display.
    return dict(grouped_pairs)