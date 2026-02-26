/*==============================================================================
APPROACH 3: Cortex AI Enrichment Pipeline
Philosophy: Use Snowflake's native LLM functions to analyze, classify, and
            enrich OpenAI response data. Meta-analysis of AI outputs using AI,
            entirely within Snowflake -- no external API calls required.

Depends on: Approach 2 Silver tables (DT_COMPLETIONS, DT_BATCH_OUTCOMES).
==============================================================================*/

USE SCHEMA SNOWFLAKE_EXAMPLE.OPENAI_DATA_ENG;
USE WAREHOUSE SFE_OPENAI_DATA_ENG_WH;

/*------------------------------------------------------------------------------
DT_ENRICHED_COMPLETIONS — Cortex-enriched completion analysis.
Classifies topic, scores sentiment, and summarizes long content.

NOTE: Cortex LLM functions consume credits. In production, consider:
  - Filtering to only new/unprocessed rows
  - Using gpt-4o-mini equivalent (snowflake-arctic or mistral-large) for cost
  - Caching results in a persistent table instead of a dynamic table
------------------------------------------------------------------------------*/

CREATE OR REPLACE DYNAMIC TABLE DT_ENRICHED_COMPLETIONS
  TARGET_LAG = '10 minutes'
  WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
  COMMENT = 'DEMO: Approach 3 - Cortex-enriched completions (Expires: 2026-03-28)'
AS
SELECT
    completion_id,
    model,
    created_at,
    finish_reason,
    content,
    content_length,
    is_refusal,
    has_tool_calls,
    is_structured_output,
    prompt_tokens,
    completion_tokens,
    total_tokens,

    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        content,
        ['technical_explanation', 'data_analysis', 'code_generation',
         'summarization', 'general_knowledge', 'recommendation']
    ):label::STRING                                         AS topic_classification,

    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        content,
        ['technical_explanation', 'data_analysis', 'code_generation',
         'summarization', 'general_knowledge', 'recommendation']
    ):score::FLOAT                                          AS topic_confidence,

    SNOWFLAKE.CORTEX.SENTIMENT(content)                     AS sentiment_score,

    CASE
        WHEN content_length > 200
        THEN SNOWFLAKE.CORTEX.SUMMARIZE(content)
        ELSE content
    END                                                     AS content_summary

FROM DT_COMPLETIONS
WHERE content IS NOT NULL
  AND is_refusal = FALSE;


/*------------------------------------------------------------------------------
DT_BATCH_ENRICHED — Classified batch results for routing validation.
Compares OpenAI's classification against Cortex's independent analysis.
This is particularly compelling: showing Snowflake can QA another AI's outputs.
------------------------------------------------------------------------------*/

CREATE OR REPLACE DYNAMIC TABLE DT_BATCH_ENRICHED
  TARGET_LAG = '10 minutes'
  WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
  COMMENT = 'DEMO: Approach 3 - Cortex QA of batch classifications (Expires: 2026-03-28)'
AS
SELECT
    batch_request_id,
    custom_id,
    outcome,
    model,
    content,
    content_parsed,
    content_parsed:category::STRING                         AS openai_category,
    content_parsed:priority::STRING                         AS openai_priority,
    content_parsed:sentiment::STRING                        AS openai_sentiment,
    content_parsed:suggested_routing::STRING                AS openai_routing,

    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        custom_id || ': ' || COALESCE(content, refusal, 'no content'),
        ['billing', 'technical_support', 'feature_request', 'account_access',
         'general_inquiry', 'outage_report', 'compliance', 'cancellation',
         'data_request']
    ):label::STRING                                         AS cortex_category,

    SNOWFLAKE.CORTEX.SENTIMENT(
        COALESCE(content, refusal, 'no content')
    )                                                       AS cortex_sentiment_score,

    IFF(content_parsed:category::STRING =
        SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
            custom_id || ': ' || COALESCE(content, refusal, 'no content'),
            ['billing', 'technical_support', 'feature_request', 'account_access',
             'general_inquiry', 'outage_report', 'compliance', 'cancellation',
             'data_request']
        ):label::STRING,
        'AGREE', 'DISAGREE')                                AS classification_agreement,

    total_tokens

FROM DT_BATCH_OUTCOMES
WHERE outcome = 'SUCCESS'
  AND content_parsed IS NOT NULL;


/*------------------------------------------------------------------------------
DT_PII_SCAN — Scan completion content and tool call arguments for PII.
Uses Cortex COMPLETE with a focused prompt to detect sensitive data patterns.
------------------------------------------------------------------------------*/

CREATE OR REPLACE DYNAMIC TABLE DT_PII_SCAN
  TARGET_LAG = '30 minutes'
  WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
  COMMENT = 'DEMO: Approach 3 - PII detection in AI outputs (Expires: 2026-03-28)'
AS
WITH completion_texts AS (
    SELECT
        completion_id       AS source_id,
        'completion'        AS source_type,
        content             AS text_to_scan,
        created_at
    FROM DT_COMPLETIONS
    WHERE content IS NOT NULL
      AND is_refusal = FALSE

    UNION ALL

    SELECT
        completion_id       AS source_id,
        'tool_call_args'    AS source_type,
        arguments_json      AS text_to_scan,
        created_at
    FROM DT_TOOL_CALLS
    WHERE arguments_json IS NOT NULL
)
SELECT
    source_id,
    source_type,
    text_to_scan,
    created_at,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        'Analyze the following text and return ONLY a JSON object with these fields: '
        || '{"has_pii": true/false, "pii_types": ["list of types found"], '
        || '"risk_level": "none/low/medium/high"}. '
        || 'PII types to check: email, phone, SSN, credit card, address, name, date of birth. '
        || 'Text to analyze: ' || LEFT(text_to_scan, 2000)
    )                                                       AS pii_analysis_raw,
    TRY_PARSE_JSON(
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            'Analyze the following text and return ONLY a JSON object with these fields: '
            || '{"has_pii": true/false, "pii_types": ["list of types found"], '
            || '"risk_level": "none/low/medium/high"}. '
            || 'PII types to check: email, phone, SSN, credit card, address, name, date of birth. '
            || 'Text to analyze: ' || LEFT(text_to_scan, 2000)
        )
    )                                                       AS pii_analysis_parsed
FROM completion_texts;


/*------------------------------------------------------------------------------
V_ENRICHMENT_DASHBOARD — Aggregated view for the Streamlit app.
Combines topic distribution, sentiment trends, and PII alerts.
------------------------------------------------------------------------------*/

CREATE OR REPLACE VIEW V_ENRICHMENT_DASHBOARD
  COMMENT = 'DEMO: Approach 3 - Enrichment dashboard aggregations (Expires: 2026-03-28)'
AS
SELECT
    topic_classification,
    COUNT(*)                                                AS response_count,
    ROUND(AVG(sentiment_score), 3)                          AS avg_sentiment,
    ROUND(AVG(topic_confidence), 3)                         AS avg_topic_confidence,
    SUM(total_tokens)                                       AS total_tokens_consumed,
    ROUND(AVG(total_tokens), 0)                             AS avg_tokens_per_response,
    SUM(IFF(has_tool_calls, 1, 0))                          AS tool_call_responses,
    SUM(IFF(is_structured_output, 1, 0))                    AS structured_output_count
FROM DT_ENRICHED_COMPLETIONS
GROUP BY topic_classification;
