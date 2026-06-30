"""
app.py

Interactive dashboard for a local, MapReduce-inspired text analytics pipeline.

The dashboard uses fictional operational documents to demonstrate:
- Parallel mapper tasks
- Shuffle and grouping behavior
- Reducer-based word aggregation
- Document profiles
- Category-level text analysis
- CSV export workflows

This project is a local educational implementation. It demonstrates the
logical stages of MapReduce without claiming to run on Hadoop or a cloud cluster.
"""

# Imports, configuration, and helper functions

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from local_pipeline.parallel_pipeline import run_local_pipeline
from mapper_reducer.mapper import tokenize_text


# Identify the folder containing app.py so the dashboard can reliably
# locate the fictional CSV dataset no matter where the command is run.
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_PATH = PROJECT_ROOT / "data" / "sample_documents.csv"


st.set_page_config(
    page_title="Distributed Text Analytics Pipeline",
    page_icon="📊",
    layout="wide",
)


@st.cache_data(show_spinner="Running local text analytics pipeline...")
def load_pipeline_results() -> dict[str, Any]:
    """
    Run the local pipeline and cache its results.

    Streamlit caching prevents the pipeline from rerunning unnecessarily
    each time a user clicks a tab, filter, or download button.
    """
    return run_local_pipeline(DATA_PATH)


def dataframe_to_csv_bytes(dataframe: pd.DataFrame) -> bytes:
    """
    Convert a DataFrame into downloadable UTF-8 CSV bytes.
    """
    return dataframe.to_csv(index=False).encode("utf-8")


def render_page_header() -> None:
    """Display the dashboard title, context, and important limitations."""
    st.title("Distributed Text Analytics Pipeline")
    st.caption(
        "A local, MapReduce-inspired dashboard for analyzing a fictional "
        "collection of operational documents."
    )

    st.info(
        "This educational portfolio project uses Python worker threads to "
        "demonstrate mapper, shuffle, and reducer stages locally. "
        "It does not claim to run on Hadoop, Spark, or a cloud cluster."
    )

    with st.expander("How the pipeline works"):
        st.markdown(
            """
            ```text
            Fictional document collection
                    ↓
            Parallel mapper tasks
                    ↓
            Intermediate word-count pairs
                    ↓
            Shuffle and grouping stage
                    ↓
            Reducer aggregation
                    ↓
            Dashboard analytics and CSV exports
            ```
            """
        )


def render_portfolio_overview(results: dict[str, Any]) -> None:
    """
    Display top-level metrics, word-frequency analysis, category analysis,
    document profiles, and downloadable CSV outputs.
    """
    metrics = results["pipeline_metrics"]
    word_counts = results["word_counts"]
    category_summary = results["category_summary"]
    document_profiles = results["document_profiles"]
    source_documents = results["source_documents"]

    st.subheader("Portfolio Overview")
    st.write(
        "This view summarizes the fictional document collection after it has "
        "passed through the local mapper, shuffle, and reducer workflow."
    )

    metric_one, metric_two, metric_three, metric_four = st.columns(4)

    metric_one.metric(
        "Documents Processed",
        metrics["documents_processed"],
    )
    metric_two.metric(
        "Mapper Pairs Created",
        f"{metrics['mapper_pairs_created']:,}",
    )
    metric_three.metric(
        "Unique Terms",
        metrics["unique_terms"],
    )
    metric_four.metric(
        "Categories Processed",
        metrics["categories_processed"],
    )

    left_column, right_column = st.columns(2)

    with left_column:
        st.markdown("### Most Frequent Terms")

        top_terms = word_counts.head(15).sort_values(
            by="count",
            ascending=True,
        )

        top_terms_chart = px.bar(
            top_terms,
            x="count",
            y="term",
            orientation="h",
            title="Top 15 Meaningful Terms Across All Documents",
            labels={
                "count": "Total Occurrences",
                "term": "Term",
            },
        )

        st.plotly_chart(
            top_terms_chart,
            width="stretch",
        )

    with right_column:
        st.markdown("### Category-Level Text Volume")

        category_chart_data = category_summary.sort_values(
            by="total_meaningful_words",
            ascending=True,
        )

        category_chart = px.bar(
            category_chart_data,
            x="total_meaningful_words",
            y="category",
            orientation="h",
            title="Meaningful Words by Document Category",
            labels={
                "total_meaningful_words": "Total Meaningful Words",
                "category": "Category",
            },
            hover_data=[
                "documents",
                "average_meaningful_words",
            ],
        )

        st.plotly_chart(
            category_chart,
            width="stretch",
        )

    st.markdown("### Document Profiles")

    profile_chart = px.scatter(
        document_profiles,
        x="word_count",
        y="unique_word_count",
        size="word_count",
        color="category",
        hover_name="title",
        hover_data=[
            "document_id",
            "source_type",
            "top_terms",
        ],
        title="Document Length and Vocabulary Diversity",
        labels={
            "word_count": "Meaningful Word Count",
            "unique_word_count": "Unique Meaningful Words",
            "category": "Category",
        },
    )

    st.plotly_chart(
        profile_chart,
        width="stretch",
    )

    st.markdown("### Downloadable Analysis Outputs")

    download_one, download_two, download_three = st.columns(3)

    with download_one:
        st.download_button(
            label="Download Word Counts CSV",
            data=dataframe_to_csv_bytes(word_counts),
            file_name="word_counts.csv",
            mime="text/csv",
            width="stretch",
        )

    with download_two:
        st.download_button(
            label="Download Document Profiles CSV",
            data=dataframe_to_csv_bytes(document_profiles),
            file_name="document_profiles.csv",
            mime="text/csv",
            width="stretch",
        )

    with download_three:
        st.download_button(
            label="Download Category Summary CSV",
            data=dataframe_to_csv_bytes(category_summary),
            file_name="category_summary.csv",
            mime="text/csv",
            width="stretch",
        )

    with st.expander("View Scored Document Profile Table"):
        st.dataframe(
            document_profiles,
            width="stretch",
            hide_index=True,
        )

    with st.expander("View Fictional Source Document Metadata"):
        st.dataframe(
            source_documents[
                [
                    "document_id",
                    "title",
                    "category",
                    "source_type",
                ]
            ],
            width="stretch",
            hide_index=True,
        )
        
        
# Pipeline stages tab
def render_pipeline_stages(results: dict[str, Any]) -> None:
    """
    Make the mapper, shuffle, and reducer stages visible to the user.
    """
    mapper_task_summary = results["mapper_task_summary"]
    grouped_pairs = results["grouped_pairs"]
    word_counts = results["word_counts"]
    metrics = results["pipeline_metrics"]

    st.subheader("Pipeline Stages")
    st.write(
        "This tab shows how the fictional documents move through each stage "
        "of the local MapReduce-inspired workflow."
    )

    st.markdown("## 1. Mapper Stage")

    st.write(
        "Each document is treated as an independent mapper task. "
        "The mapper cleans the document text, removes common stop words, "
        "and emits intermediate key-value pairs such as `('service', 1)`."
    )

    mapper_metric_one, mapper_metric_two = st.columns(2)

    mapper_metric_one.metric(
        "Mapper Tasks",
        len(mapper_task_summary),
    )
    mapper_metric_two.metric(
        "Total Intermediate Pairs",
        f"{metrics['mapper_pairs_created']:,}",
    )

    mapper_chart = px.bar(
        mapper_task_summary.sort_values(
            by="mapper_pairs_created",
            ascending=False,
        ),
        x="mapper_pairs_created",
        y="title",
        color="category",
        orientation="h",
        title="Intermediate Word-Count Pairs Created by Mapper Task",
        labels={
            "mapper_pairs_created": "Mapper Pairs Created",
            "title": "Document",
            "category": "Category",
        },
    )

    st.plotly_chart(
        mapper_chart,
        width="stretch",
    )

    with st.expander("View Mapper Task Summary"):
        st.dataframe(
            mapper_task_summary.sort_values(
                by="mapper_pairs_created",
                ascending=False,
            ),
            width="stretch",
            hide_index=True,
        )

    st.markdown("## 2. Shuffle and Grouping Stage")

    st.write(
        "The shuffle stage groups matching words together before reduction. "
        "For example, multiple `('service', 1)` pairs become one grouped entry "
        "such as `service: [1, 1, 1]`."
    )

    grouping_preview_rows = []

    sorted_grouped_pairs = sorted(
        grouped_pairs.items(),
        key=lambda item: (-len(item[1]), item[0]),
    )

    for term, values in sorted_grouped_pairs[:20]:
        grouping_preview_rows.append(
            {
                "term": term,
                "intermediate_values": ", ".join(
                    str(value) for value in values
                ),
                "pair_count": len(values),
            }
        )

    grouping_preview = pd.DataFrame(grouping_preview_rows)

    shuffle_metric_one, shuffle_metric_two = st.columns(2)

    shuffle_metric_one.metric(
        "Grouped Terms",
        len(grouped_pairs),
    )
    shuffle_metric_two.metric(
        "Largest Term Group",
        int(grouping_preview["pair_count"].max()),
    )

    st.dataframe(
        grouping_preview,
        width="stretch",
        hide_index=True,
    )

    st.markdown("## 3. Reducer Stage")

    st.write(
        "The reducer sums the grouped values to produce final term totals. "
        "For example, `service: [1, 1, 1]` becomes `service: 3`."
    )

    reducer_chart = px.bar(
        word_counts.head(20).sort_values(
            by="count",
            ascending=True,
        ),
        x="count",
        y="term",
        orientation="h",
        title="Reducer Output: Top 20 Final Word Totals",
        labels={
            "count": "Final Count",
            "term": "Term",
        },
    )

    st.plotly_chart(
        reducer_chart,
        width="stretch",
    )

    with st.expander("View Full Reducer Output"):
        st.dataframe(
            word_counts,
            width="stretch",
            hide_index=True,
        )
        
# Document Explorer and application launcher

def render_document_explorer(results: dict[str, Any]) -> None:
    """
    Allow users to inspect one fictional source document at a time.
    """
    source_documents = results["source_documents"]
    document_profiles = results["document_profiles"]

    st.subheader("Document Explorer")
    st.write(
        "Select a fictional source document to inspect its text, metadata, "
        "meaningful-word profile, and local mapper output."
    )

    document_options = source_documents.apply(
        lambda row: f"{row['document_id']} — {row['title']}",
        axis=1,
    ).tolist()

    selected_option = st.selectbox(
        "Select a fictional document",
        options=document_options,
    )

    selected_document_id = selected_option.split(" — ", maxsplit=1)[0]

    selected_document = source_documents.loc[
        source_documents["document_id"] == selected_document_id
    ].iloc[0]

    selected_profile = document_profiles.loc[
        document_profiles["document_id"] == selected_document_id
    ].iloc[0]

    metric_one, metric_two, metric_three = st.columns(3)

    metric_one.metric(
        "Meaningful Words",
        int(selected_profile["word_count"]),
    )
    metric_two.metric(
        "Unique Meaningful Words",
        int(selected_profile["unique_word_count"]),
    )
    metric_three.metric(
        "Category",
        selected_document["category"],
    )

    metadata_one, metadata_two = st.columns(2)

    with metadata_one:
        st.markdown("### Document Metadata")
        st.write(f"**Document ID:** {selected_document['document_id']}")
        st.write(f"**Title:** {selected_document['title']}")
        st.write(f"**Category:** {selected_document['category']}")
        st.write(f"**Source Type:** {selected_document['source_type']}")

    with metadata_two:
        st.markdown("### Top Meaningful Terms")
        st.write(
            selected_profile["top_terms"]
            if selected_profile["top_terms"]
            else "No meaningful terms found."
        )

    st.markdown("### Fictional Source Text")
    st.info(selected_document["text"])

    tokens = tokenize_text(str(selected_document["text"]))
    token_counts = Counter(tokens)

    document_term_counts = pd.DataFrame(
        [
            {
                "term": term,
                "count": count,
            }
            for term, count in token_counts.items()
        ]
    ).sort_values(
        by=["count", "term"],
        ascending=[False, True],
    )

    st.markdown("### Mapper Output for Selected Document")

    st.write(
        "The local mapper converts the selected document into normalized "
        "word-count pairs. The chart below summarizes that intermediate output."
    )

    document_chart = px.bar(
        document_term_counts.head(15).sort_values(
            by="count",
            ascending=True,
        ),
        x="count",
        y="term",
        orientation="h",
        title="Top Terms Produced by the Selected Mapper Task",
        labels={
            "count": "Mapper Term Count",
            "term": "Term",
        },
    )

    st.plotly_chart(
        document_chart,
        width="stretch",
    )

    with st.expander("View Selected Document Term Counts"):
        st.dataframe(
            document_term_counts,
            width="stretch",
            hide_index=True,
        )

    st.download_button(
        label="Download Selected Document Term Counts",
        data=dataframe_to_csv_bytes(document_term_counts),
        file_name=f"{selected_document_id.lower()}_term_counts.csv",
        mime="text/csv",
    )


def main() -> None:
    """Run the Streamlit dashboard."""
    render_page_header()

    if st.sidebar.button("Refresh Pipeline Results"):
        st.cache_data.clear()
        st.rerun()

    try:
        results = load_pipeline_results()
    except Exception as error:
        st.error("The dashboard could not run the local text analytics pipeline.")
        st.exception(error)
        return

    overview_tab, pipeline_tab, explorer_tab = st.tabs(
        [
            "Portfolio Overview",
            "Pipeline Stages",
            "Document Explorer",
        ]
    )

    with overview_tab:
        render_portfolio_overview(results)

    with pipeline_tab:
        render_pipeline_stages(results)

    with explorer_tab:
        render_document_explorer(results)


if __name__ == "__main__":
    main()
    
    
