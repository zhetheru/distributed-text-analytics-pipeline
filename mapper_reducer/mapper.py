"""
mapper.py

This module contains the mapper stage for a local, MapReduce-inspired
text analytics pipeline.

The mapper processes one document at a time and produces:
1. Word-count key-value pairs, such as ("service", 1)
2. Basic document-level statistics for later analysis
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any


# These common words add little analytical value in a small document collection.
# Removing them helps the output emphasize meaningful operational terms.
DEFAULT_STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "before",
    "by",
    "can",
    "for",
    "from",
    "helps",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "should",
    "that",
    "the",
    "this",
    "to",
    "use",
    "when",
    "will",
    "with",
}


def tokenize_text(text: str, stop_words: set[str] | None = None) -> list[str]:
    """
    Convert text into normalized analytical tokens.

    The function:
    - Converts text to lowercase
    - Keeps alphabetic words only
    - Removes short words and common stop words

    Parameters
    ----------
    text:
        The document text to process.
    stop_words:
        Optional custom stop-word set. If omitted, DEFAULT_STOP_WORDS is used.

    Returns
    -------
    list[str]
        A cleaned list of meaningful tokens.
    """
    if not isinstance(text, str):
        return []

    active_stop_words = stop_words if stop_words is not None else DEFAULT_STOP_WORDS

    # Find alphabetic word sequences. For example, "check-in" becomes
    # "check" and "in"; "in" is later removed as a stop word.
    raw_tokens = re.findall(r"[a-zA-Z]+", text.lower())

    return [
        token
        for token in raw_tokens
        if len(token) > 2 and token not in active_stop_words
    ]


def map_document(document: dict[str, Any]) -> list[tuple[str, int]]:
    """
    Produce word-count key-value pairs for one document.

    Example output:
    [("service", 1), ("guest", 1), ("service", 1)]

    Repeated words remain repeated here. The reducer stage will aggregate
    them into final counts later in the pipeline.
    """
    tokens = tokenize_text(str(document.get("text", "")))
    return [(token, 1) for token in tokens]


def create_document_profile(document: dict[str, Any]) -> dict[str, Any]:
    """
    Create document-level statistics that will later appear in the dashboard.

    Returns a dictionary containing identifying information, total word count,
    unique word count, and the five most common meaningful words.
    """
    tokens = tokenize_text(str(document.get("text", "")))
    token_counts = Counter(tokens)

    top_terms = ", ".join(
        word for word, _count in token_counts.most_common(5)
    )

    return {
        "document_id": document.get("document_id", ""),
        "title": document.get("title", ""),
        "category": document.get("category", ""),
        "source_type": document.get("source_type", ""),
        "word_count": len(tokens),
        "unique_word_count": len(token_counts),
        "top_terms": top_terms,
    }