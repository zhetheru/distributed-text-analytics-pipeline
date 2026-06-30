"""
test_pipeline.py

Automated tests for the local, MapReduce-inspired text analytics pipeline.
"""

from pathlib import Path

from local_pipeline.parallel_pipeline import run_local_pipeline
from mapper_reducer.mapper import map_document, tokenize_text
from mapper_reducer.reducer import reduce_word_counts
from mapper_reducer.shuffle import shuffle_and_group


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "sample_documents.csv"


def test_tokenize_text_removes_stop_words_and_normalizes_words():
    """The tokenizer should lowercase words and remove common stop words."""
    text = "Guest service teams should document service issues quickly."

    tokens = tokenize_text(text)

    assert tokens == [
        "guest",
        "service",
        "teams",
        "document",
        "service",
        "issues",
        "quickly",
    ]
    assert "should" not in tokens


def test_mapper_shuffle_and_reducer_count_repeated_words():
    """Repeated words should become a correct final reducer total."""
    document = {
        "text": "Guest service teams document service issues quickly."
    }

    mapped_pairs = map_document(document)
    grouped_pairs = shuffle_and_group(mapped_pairs)
    reduced_counts = reduce_word_counts(grouped_pairs)

    assert ("service", 1) in mapped_pairs
    assert grouped_pairs["service"] == [1, 1]
    assert reduced_counts["service"] == 2
    assert reduced_counts["guest"] == 1


def test_full_pipeline_processes_fictional_document_collection():
    """The full pipeline should process all documents and produce analytics."""
    results = run_local_pipeline(DATA_PATH)

    metrics = results["pipeline_metrics"]
    word_counts = results["word_counts"]
    document_profiles = results["document_profiles"]
    category_summary = results["category_summary"]

    assert metrics["documents_processed"] == 12
    assert metrics["categories_processed"] == 10
    assert metrics["mapper_pairs_created"] > 100
    assert metrics["unique_terms"] > 80

    assert len(document_profiles) == 12
    assert len(category_summary) == 10
    assert "service" in word_counts["term"].values


def test_word_counts_are_sorted_from_highest_to_lowest():
    """Dashboard word-count data should be sorted by frequency."""
    results = run_local_pipeline(DATA_PATH)

    counts = results["word_counts"]["count"].tolist()

    assert counts == sorted(counts, reverse=True)