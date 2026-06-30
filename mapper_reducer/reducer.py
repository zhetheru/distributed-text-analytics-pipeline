"""
reducer.py

This module contains the reducer stage for the local, MapReduce-inspired
text analytics pipeline.

The reducer receives grouped word-count values and produces final totals.
"""

from __future__ import annotations

from typing import Any


def reduce_word_counts(grouped_pairs: dict[str, list[int]]) -> dict[str, int]:
    """
    Add all values associated with each word.

    Example input:
    {
        "service": [1, 1],
        "guest": [1],
    }

    Example output:
    {
        "service": 2,
        "guest": 1,
    }
    """
    return {
        word: sum(counts)
        for word, counts in grouped_pairs.items()
    }


def get_top_terms(
    reduced_counts: dict[str, int],
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Convert final word counts into a sorted list for dashboards and CSV export.

    Parameters
    ----------
    reduced_counts:
        Dictionary containing word-frequency totals.
    limit:
        Maximum number of high-frequency terms to return.

    Returns
    -------
    list[dict[str, Any]]
        Each dictionary contains a word and its total count.
    """
    sorted_terms = sorted(
        reduced_counts.items(),
        key=lambda item: (-item[1], item[0]),
    )

    return [
        {
            "term": word,
            "count": count,
        }
        for word, count in sorted_terms[:limit]
    ]