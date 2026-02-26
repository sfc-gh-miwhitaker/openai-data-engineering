# OpenAI Data Engineering in Snowflake

![Expires](https://img.shields.io/badge/Expires-2026--03--28-orange)

> DEMONSTRATION PROJECT - EXPIRES: 2026-03-28
> This demo uses Snowflake features current as of February 2026.
> After expiration, a warning banner will be added to this README and deploy_all.sql.

Three approaches to engineering and transforming complex nested JSON data from OpenAI APIs.

**Author:** SE Community
**Created:** 2026-02-26 | **Expires:** 2026-03-28 | **Status:** ACTIVE

## First Time Here?

1. **Deploy** - Copy `deploy_all.sql` into Snowsight, click "Run All"
2. **Explore** - Query the views and dynamic tables created by each approach
3. **Cortex** - Optionally deploy `sql/03_transformations/03_approach3_cortex.sql` for AI enrichment
4. **Streamlit** - Upload `streamlit/app.py` as a Streamlit in Snowflake app
5. **Cleanup** - Run `teardown_all.sql` when done

## The Problem

OpenAI API responses are deeply nested JSON with variable schemas:

```
response
  ├── choices[]                    ← array of alternatives
  │   ├── message
  │   │   ├── content              ← string OR null
  │   │   ├── refusal              ← present only on policy violations
  │   │   └── tool_calls[]         ← optional array of function invocations
  │   │       └── function
  │   │           ├── name
  │   │           └── arguments    ← JSON string inside a JSON string
  │   └── finish_reason            ← stop | length | tool_calls | content_filter
  └── usage
      ├── prompt_tokens
      ├── completion_tokens
      └── prompt_tokens_details    ← nested sub-object with cached/audio tokens
          └── cached_tokens
```

This demo covers three data formats: **Chat Completions**, **Batch API output**, and **Usage API buckets**.

## Three Approaches

### Approach 1: Schema-on-Read (FLATTEN + Views)

Keep raw VARIANT intact. Create views that flatten on demand.

| Strength | Trade-off |
|----------|-----------|
| Zero ETL lag | Query cost on every read |
| Schema evolution tolerant | Complex view definitions |
| No storage duplication | No pre-computed aggregations |

**Objects:** `V_COMPLETIONS`, `V_TOOL_CALLS`, `V_STRUCTURED_OUTPUTS`, `V_BATCH_RESULTS`, `V_TOKEN_USAGE`

### Approach 2: Medallion Architecture (Dynamic Tables)

Declarative Bronze-Silver-Gold pipeline with automatic incremental refresh.

| Strength | Trade-off |
|----------|-----------|
| Pre-computed, fast reads | Additional storage |
| Automatic refresh via TARGET_LAG | Warehouse must be available |
| Clear dependency chain | Slight data latency (configurable) |

**Silver:** `DT_COMPLETIONS`, `DT_TOOL_CALLS`, `DT_BATCH_OUTCOMES`, `DT_USAGE_FLAT`
**Gold:** `DT_DAILY_TOKEN_SUMMARY`, `DT_TOOL_CALL_ANALYTICS`, `DT_BATCH_SUMMARY`

### Approach 3: Cortex AI Enrichment

Use Snowflake Cortex to classify, score, summarize, and scan OpenAI outputs.

| Strength | Trade-off |
|----------|-----------|
| Native AI, no external APIs | Cortex credit consumption |
| QA one AI's output with another | Region/model availability |
| PII detection built-in | Latency per enrichment call |

**Objects:** `DT_ENRICHED_COMPLETIONS`, `DT_BATCH_ENRICHED`, `DT_PII_SCAN`, `V_ENRICHMENT_DASHBOARD`

## Key Techniques Demonstrated

- `LATERAL FLATTEN` with `OUTER => TRUE` for optional arrays
- `TRY_PARSE_JSON` for safely parsing JSON-as-string (tool call arguments, structured outputs)
- Dot-notation traversal of deeply nested paths (`raw:choices[0].message.tool_calls`)
- Dynamic tables with `TARGET_LAG` for declarative pipelines
- `SNOWFLAKE.CORTEX.CLASSIFY_TEXT`, `SENTIMENT`, `SUMMARIZE`, `COMPLETE` for AI enrichment
- `IFF` / `CASE` for polymorphic field handling (content vs refusal vs tool_calls)

## Project Structure

```
openai-data-engineering/
├── README.md
├── deploy_all.sql                            # Single-file deployment
├── teardown_all.sql                          # Complete cleanup
├── diagrams/
│   └── data-flow.md                          # Architecture diagram
├── sql/
│   ├── 01_setup/
│   │   └── 01_create_schema.sql
│   ├── 02_tables/
│   │   ├── 01_create_tables.sql
│   │   └── 02_load_sample_data.sql           # Full synthetic dataset
│   ├── 03_transformations/
│   │   ├── 01_approach1_views.sql            # FLATTEN + Views
│   │   ├── 02_approach2_dynamic_tables.sql   # Dynamic Table pipeline
│   │   └── 03_approach3_cortex.sql           # Cortex enrichment
│   └── 99_cleanup/
│       └── 01_drop_objects.sql
└── streamlit/
    └── app.py                                # Interactive explorer
```
