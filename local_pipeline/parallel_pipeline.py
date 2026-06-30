"""
parallel_pipeline.py

This module coordinates a local, MapReduce-inspired text analytics pipeline.

The pipeline:
1. Loads fictional documents from a CSV file
2. Runs mapper tasks in parallel using local worker threads
3. Combines mapper output
4. Groups matching terms in a shuffle stage
5. Reduces grouped values into final word counts
6. Produces tables for dashboards, CSV exports, and testing

This is a local educational implementation. It demonstrates the logical
MapReduce workflow without claiming to run on a Hadoop or cloud cluster.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pandas as pd

from mapper_reducer.mapper import create_document_profile, map_document
from mapper_reducer.reducer import get_top_terms, reduce_word_counts
from mapper_reducer.shuffle import shuffle_and_group


REQUIRED_COLUMNS = {
    "document_id",
    "title",
    "category",
    "source_type",
    "text",
}


def load_documents(data_path: str | Path) -> pd.DataFrame:
    """
    Load and validate the fictional document dataset.

    Parameters
    ----------
    data_path:
        Path to the CSV file containing the document collection.

    Returns
    -------
    pandas.DataFrame
        A cleaned document dataset ready for pipeline processing.
    """
    file_path = Path(data_path)

    if not file_path.exists():
        raise FileNotFoundError(
            f"Document dataset was not found: {file_path}"
        )

    documents_df = pd.read_csv(file_path)

    missing_columns = REQUIRED_COLUMNS.difference(documents_df.columns)

    if missing_columns:
        missing_column_list = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"Dataset is missing required columns: {missing_column_list}"
        )

    if documents_df.empty:
        raise ValueError("The document dataset is empty.")

    # Prevent missing text or metadata values from causing errors later.
    return documents_df.fillna("")


def create_mapper_task_summary(
    documents: list[dict[str, Any]],
    mapped_outputs: list[list[tuple[str, int]]],
) -> pd.DataFrame:
    """
    Create one summary row for each mapper task.

    This makes the parallel processing stage visible in the dashboard:
    each document is treated as an independent mapper task.
    """
    summary_rows = []

    for document, mapped_pairs in zip(documents, mapped_outputs):
        summary_rows.append(
            {
                "document_id": document.get("document_id", ""),
                "title": document.get("title", ""),
                "category": document.get("category", ""),
                "mapper_pairs_created": len(mapped_pairs),
            }
        )

    return pd.DataFrame(summary_rows)


def create_category_summary(document_profiles: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize processed documents by category.

    The result helps show which operational topics contain the greatest
    volume of meaningful text in the fictional document collection.
    """
    category_summary = (
        document_profiles.groupby("category", as_index=False)
        .agg(
            documents=("document_id", "count"),
            total_meaningful_words=("word_count", "sum"),
            average_meaningful_words=("word_count", "mean"),
        )
        .sort_values(
            by="total_meaningful_words",
            ascending=False,
        )
        .reset_index(drop=True)
    )

    category_summary["average_meaningful_words"] = (
        category_summary["average_meaningful_words"].round(1)
    )

    return category_summary


def run_local_pipeline(
    data_path: str | Path,
    max_workers: int | None = None,
) -> dict[str, Any]:
    """
    Run the full local, MapReduce-inspired text analytics pipeline.

    Parameters
    ----------
    data_path:
        Path to the document CSV file.
    max_workers:
        Optional number of local mapper workers. When omitted, Python
        chooses an appropriate number for the current computer.

    Returns
    -------
    dict[str, Any]
        A dictionary containing source documents, mapper output summaries,
        grouped terms, final word counts, document profiles, category
        summaries, and high-level pipeline metrics.
    """
    documents_df = load_documents(data_path)
    documents = documents_df.to_dict(orient="records")

    # Each document is independently processed by a mapper task.
    # ThreadPoolExecutor is used for a safe, cross-platform local demo.
    with ThreadPoolExecutor(max_workers=max_workers) as mapper_pool:
        mapped_outputs = list(mapper_pool.map(map_document, documents))

    # Document profiles are also independent per document, so they can
    # be created in parallel using the same local-worker concept.
    with ThreadPoolExecutor(max_workers=max_workers) as profile_pool:
        document_profiles = list(
            profile_pool.map(create_document_profile, documents)
        )

    # Combine all mapper pairs into one intermediate collection.
    all_mapped_pairs = [
        pair
        for document_pairs in mapped_outputs
        for pair in document_pairs
    ]

    # Shuffle groups matching terms before the reducer calculates totals.
    grouped_pairs = shuffle_and_group(all_mapped_pairs)
    reduced_counts = reduce_word_counts(grouped_pairs)

    # Convert outputs into dashboard-ready DataFrames.
    word_counts = pd.DataFrame(
        get_top_terms(
            reduced_counts,
            limit=len(reduced_counts),
        )
    )

    document_profiles_df = pd.DataFrame(document_profiles)

    mapper_task_summary = create_mapper_task_summary(
        documents,
        mapped_outputs,
    )

    category_summary = create_category_summary(document_profiles_df)

    pipeline_metrics = {
        "documents_processed": len(documents),
        "mapper_pairs_created": len(all_mapped_pairs),
        "unique_terms": len(reduced_counts),
        "categories_processed": documents_df["category"].nunique(),
    }

    return {
        "source_documents": documents_df,
        "mapper_task_summary": mapper_task_summary,
        "mapped_pairs": all_mapped_pairs,
        "grouped_pairs": grouped_pairs,
        "word_counts": word_counts,
        "document_profiles": document_profiles_df,
        "category_summary": category_summary,
        "pipeline_metrics": pipeline_metrics,
    }