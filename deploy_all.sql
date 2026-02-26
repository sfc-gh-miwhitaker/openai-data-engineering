/*==============================================================================
DEPLOY ALL - OpenAI Data Engineering
Author: SE Community | Expires: 2026-03-28
INSTRUCTIONS: Open in Snowsight → Click "Run All"

Three approaches to engineering complex OpenAI API response data:
  1. Schema-on-Read (FLATTEN + Views)
  2. Medallion Architecture (Dynamic Tables)
  3. Cortex AI Enrichment Pipeline
==============================================================================*/

-- Expiration check (informational — warns but does not block)
SELECT
    '2026-03-28'::DATE AS expiration_date,
    CURRENT_DATE() AS current_date,
    DATEDIFF('day', CURRENT_DATE(), '2026-03-28'::DATE) AS days_remaining,
    CASE
        WHEN DATEDIFF('day', CURRENT_DATE(), '2026-03-28'::DATE) < 0
        THEN 'EXPIRED - Code may use outdated syntax. Remove expiration banner to continue.'
        WHEN DATEDIFF('day', CURRENT_DATE(), '2026-03-28'::DATE) <= 7
        THEN 'EXPIRING SOON - ' || DATEDIFF('day', CURRENT_DATE(), '2026-03-28'::DATE) || ' days remaining'
        ELSE 'ACTIVE - ' || DATEDIFF('day', CURRENT_DATE(), '2026-03-28'::DATE) || ' days remaining'
    END AS demo_status;

-------------------------------------------------------------------------------
-- 1. SETUP
-------------------------------------------------------------------------------

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_EXAMPLE;

CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_EXAMPLE.OPENAI_DATA_ENG
  COMMENT = 'DEMO: OpenAI API data engineering patterns (Expires: 2026-03-28)';

CREATE WAREHOUSE IF NOT EXISTS SFE_OPENAI_DATA_ENG_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'DEMO: OpenAI data engineering compute (Expires: 2026-03-28)';

USE SCHEMA SNOWFLAKE_EXAMPLE.OPENAI_DATA_ENG;
USE WAREHOUSE SFE_OPENAI_DATA_ENG_WH;

-------------------------------------------------------------------------------
-- 2. RAW TABLES
-------------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_CHAT_COMPLETIONS (
  loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  source_file VARCHAR,
  raw         VARIANT
) COMMENT = 'DEMO: Raw OpenAI Chat Completions API responses (Expires: 2026-03-28)';

CREATE OR REPLACE TABLE RAW_BATCH_OUTPUTS (
  loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  source_file VARCHAR,
  raw         VARIANT
) COMMENT = 'DEMO: Raw OpenAI Batch API output records (Expires: 2026-03-28)';

CREATE OR REPLACE TABLE RAW_USAGE_BUCKETS (
  loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  source_file VARCHAR,
  raw         VARIANT
) COMMENT = 'DEMO: Raw OpenAI Usage API bucket records (Expires: 2026-03-28)';

-------------------------------------------------------------------------------
-- 3. SAMPLE DATA (see sql/02_tables/02_load_sample_data.sql for full dataset)
--    Inline here for single-file deployment.
-------------------------------------------------------------------------------

-- Chat Completions: text responses
INSERT INTO RAW_CHAT_COMPLETIONS (source_file, raw)
SELECT 'api_log_2026-02-20.jsonl', PARSE_JSON(column1) FROM VALUES
('{"id":"chatcmpl-9a1b2c3d","object":"chat.completion","created":1740067200,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":"Snowflake''s VARIANT data type stores semi-structured data in a compressed columnar format, supporting up to 16 MB per value. It automatically handles schema detection and enables dot-notation traversal for JSON, Avro, Parquet, and XML.","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":42,"completion_tokens":58,"total_tokens":100,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_a1b2c3d4"}'),
('{"id":"chatcmpl-e4f5g6h7","object":"chat.completion","created":1740067260,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":"Based on the Q4 earnings data, revenue grew 12% year-over-year to $2.1B. Key drivers include: (1) cloud migration acceleration in financial services, (2) 34% growth in consumption-based contracts, and (3) expansion in the APAC region. Operating margins improved 200bps to 28%.","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":1250,"completion_tokens":312,"total_tokens":1562,"prompt_tokens_details":{"cached_tokens":1024,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":128,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_e4f5g6h7"}'),
('{"id":"chatcmpl-m2n3o4p5","object":"chat.completion","created":1740067380,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":null,"refusal":"I''m unable to help with that request as it involves generating content that could be harmful."},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":35,"completion_tokens":22,"total_tokens":57,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_m2n3o4p5"}'),
('{"id":"chatcmpl-q6r7s8t9","object":"chat.completion","created":1740067440,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":"Machine learning pipelines typically involve several stages: data ingestion, feature engineering, model training, validation, and deployment. In the data ingestion phase, raw data is collected from various sources including databases, APIs, and streaming platforms. Feature engineering transforms this raw data into","refusal":null},"logprobs":null,"finish_reason":"length"}],"usage":{"prompt_tokens":56,"completion_tokens":50,"total_tokens":106,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_q6r7s8t9"}'),
('{"id":"chatcmpl-u0v1w2x3","object":"chat.completion","created":1740067500,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":"Option A: Use a medallion architecture with bronze, silver, and gold layers for progressive data refinement.","refusal":null},"logprobs":null,"finish_reason":"stop"},{"index":1,"message":{"role":"assistant","content":"Option B: Implement a hub-and-spoke model where raw data lands in a central repository and domain-specific views serve each team.","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":88,"completion_tokens":64,"total_tokens":152,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_u0v1w2x3"}'),
('{"id":"chatcmpl-y4z5a6b7","object":"chat.completion","created":1740067560,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":"```python\nimport pandas as pd\nfrom sklearn.ensemble import RandomForestClassifier\nfrom sklearn.model_selection import train_test_split\n\ndef train_model(df: pd.DataFrame, target_col: str) -> RandomForestClassifier:\n    X = df.drop(columns=[target_col])\n    y = df[target_col]\n    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)\n    model = RandomForestClassifier(n_estimators=100)\n    model.fit(X_train, y_train)\n    print(f\"Accuracy: {model.score(X_test, y_test):.3f}\")\n    return model\n```","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":94,"completion_tokens":186,"total_tokens":280,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_y4z5a6b7"}'),
('{"id":"chatcmpl-c8d9e0f1","object":"chat.completion","created":1740067620,"model":"gpt-4o-mini-2024-07-18","choices":[{"index":0,"message":{"role":"assistant","content":"TL;DR: The report recommends migrating legacy on-prem data warehouses to a cloud-native architecture within 18 months. Key benefits include 40% cost reduction, 3x query performance, and elastic scaling.","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":4200,"completion_tokens":72,"total_tokens":4272,"prompt_tokens_details":{"cached_tokens":4096,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_c8d9e0f1"}');

-- Chat Completions: tool calls
INSERT INTO RAW_CHAT_COMPLETIONS (source_file, raw)
SELECT 'api_log_2026-02-20.jsonl', PARSE_JSON(column1) FROM VALUES
('{"id":"chatcmpl-tc01","object":"chat.completion","created":1740068000,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_w001","type":"function","function":{"name":"get_weather","arguments":"{\"location\":\"San Francisco, CA\",\"unit\":\"fahrenheit\"}"}}],"refusal":null},"logprobs":null,"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":82,"completion_tokens":28,"total_tokens":110,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_tc01"}'),
('{"id":"chatcmpl-tc03","object":"chat.completion","created":1740068120,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_m001","type":"function","function":{"name":"get_stock_price","arguments":"{\"symbol\":\"SNOW\"}"}},{"id":"call_m002","type":"function","function":{"name":"get_stock_price","arguments":"{\"symbol\":\"MSFT\"}"}},{"id":"call_m003","type":"function","function":{"name":"get_company_news","arguments":"{\"company\":\"Snowflake Inc\",\"days\":7}"}}],"refusal":null},"logprobs":null,"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":210,"completion_tokens":95,"total_tokens":305,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_tc03"}'),
('{"id":"chatcmpl-tc04","object":"chat.completion","created":1740068180,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_c001","type":"function","function":{"name":"create_dashboard","arguments":"{\"title\":\"Q4 Revenue Overview\",\"charts\":[{\"type\":\"bar\",\"metric\":\"revenue\",\"groupBy\":\"region\",\"filters\":{\"quarter\":\"Q4\",\"year\":2025}},{\"type\":\"line\",\"metric\":\"growth_rate\",\"groupBy\":\"month\",\"filters\":{\"year\":2025}}],\"sharing\":{\"teams\":[\"finance\",\"executive\"],\"public\":false}}"}}],"refusal":null},"logprobs":null,"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":340,"completion_tokens":148,"total_tokens":488,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":64,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_tc04"}'),
('{"id":"chatcmpl-so01","object":"chat.completion","created":1740068300,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":"{\"sentiment\":\"positive\",\"confidence\":0.92,\"topics\":[\"product_quality\",\"customer_service\"],\"entities\":[{\"name\":\"Acme Widget Pro\",\"type\":\"product\"},{\"name\":\"Sarah Chen\",\"type\":\"person\"}],\"summary\":\"Customer expresses high satisfaction with product quality and support responsiveness.\"}","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":520,"completion_tokens":95,"total_tokens":615,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_so01"}'),
('{"id":"chatcmpl-so02","object":"chat.completion","created":1740068360,"model":"gpt-4o-2024-08-06","choices":[{"index":0,"message":{"role":"assistant","content":"{\"invoice_number\":\"INV-2026-0847\",\"vendor\":\"CloudTech Solutions\",\"date\":\"2026-01-15\",\"line_items\":[{\"description\":\"Cloud Compute (Jan)\",\"quantity\":1,\"unit_price\":4500.00,\"total\":4500.00},{\"description\":\"Storage (500TB)\",\"quantity\":500,\"unit_price\":23.00,\"total\":11500.00},{\"description\":\"Support Premium\",\"quantity\":1,\"unit_price\":2000.00,\"total\":2000.00}],\"subtotal\":18000.00,\"tax\":1620.00,\"total\":19620.00,\"currency\":\"USD\"}","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":890,"completion_tokens":210,"total_tokens":1100,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"system_fingerprint":"fp_so02"}');

-- Batch API outputs
INSERT INTO RAW_BATCH_OUTPUTS (source_file, raw)
SELECT 'batch_output_20260220.jsonl', PARSE_JSON(column1) FROM VALUES
('{"id":"batch_req_001","custom_id":"ticket-classify-1001","response":{"status_code":200,"request_id":"req_aaa111","body":{"id":"chatcmpl-b001","object":"chat.completion","created":1740070000,"model":"gpt-4o-mini-2024-07-18","choices":[{"index":0,"message":{"role":"assistant","content":"{\"category\":\"billing\",\"priority\":\"high\",\"sentiment\":\"negative\",\"suggested_routing\":\"billing_escalation\"}","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":245,"completion_tokens":38,"total_tokens":283},"system_fingerprint":"fp_batch01"}},"error":null}'),
('{"id":"batch_req_002","custom_id":"ticket-classify-1002","response":{"status_code":200,"request_id":"req_bbb222","body":{"id":"chatcmpl-b002","object":"chat.completion","created":1740070001,"model":"gpt-4o-mini-2024-07-18","choices":[{"index":0,"message":{"role":"assistant","content":"{\"category\":\"technical_support\",\"priority\":\"medium\",\"sentiment\":\"neutral\",\"suggested_routing\":\"tier2_support\"}","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":312,"completion_tokens":42,"total_tokens":354},"system_fingerprint":"fp_batch01"}},"error":null}'),
('{"id":"batch_req_003","custom_id":"ticket-classify-1003","response":{"status_code":200,"request_id":"req_ccc333","body":{"id":"chatcmpl-b003","object":"chat.completion","created":1740070002,"model":"gpt-4o-mini-2024-07-18","choices":[{"index":0,"message":{"role":"assistant","content":"{\"category\":\"feature_request\",\"priority\":\"low\",\"sentiment\":\"positive\",\"suggested_routing\":\"product_feedback\"}","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":189,"completion_tokens":35,"total_tokens":224},"system_fingerprint":"fp_batch01"}},"error":null}'),
('{"id":"batch_req_006","custom_id":"ticket-classify-1006","response":null,"error":{"code":"rate_limit_exceeded","message":"Rate limit reached for gpt-4o-mini in organization org-abc123 on tokens per min. Limit: 200000, Used: 198542, Requested: 3200."}}'),
('{"id":"batch_req_007","custom_id":"ticket-classify-1007","response":null,"error":{"code":"server_error","message":"The server had an error processing your request. Sorry about that!"}}'),
('{"id":"batch_req_009","custom_id":"ticket-classify-1009","response":{"status_code":200,"request_id":"req_ggg777","body":{"id":"chatcmpl-b009","object":"chat.completion","created":1740070008,"model":"gpt-4o-mini-2024-07-18","choices":[{"index":0,"message":{"role":"assistant","content":"{\"category\":\"outage_report\",\"priority\":\"critical\",\"sentiment\":\"negative\",\"suggested_routing\":\"incident_response\"}","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":502,"completion_tokens":42,"total_tokens":544},"system_fingerprint":"fp_batch01"}},"error":null}'),
('{"id":"batch_req_012","custom_id":"ticket-classify-1012","response":{"status_code":200,"request_id":"req_jjj000","body":{"id":"chatcmpl-b012","object":"chat.completion","created":1740070011,"model":"gpt-4o-mini-2024-07-18","choices":[{"index":0,"message":{"role":"assistant","content":"{\"category\":\"cancellation\",\"priority\":\"high\",\"sentiment\":\"negative\",\"suggested_routing\":\"retention_team\"}","refusal":null},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":198,"completion_tokens":37,"total_tokens":235},"system_fingerprint":"fp_batch01"}},"error":null}');

-- Usage API buckets
INSERT INTO RAW_USAGE_BUCKETS (source_file, raw)
SELECT 'usage_export_20260220.json', PARSE_JSON(column1) FROM VALUES
('{"object":"bucket","start_time":1739836800,"end_time":1739923200,"results":[{"object":"organization.usage.completions.result","input_tokens":285400,"output_tokens":142300,"num_model_requests":3420,"input_cached_tokens":82100,"project_id":"proj_customer_support","user_id":null,"api_key_id":"key_prod_001","model":"gpt-4o-2024-08-06","batch":false},{"object":"organization.usage.completions.result","input_tokens":98200,"output_tokens":45600,"num_model_requests":1250,"input_cached_tokens":12000,"project_id":"proj_customer_support","user_id":null,"api_key_id":"key_prod_001","model":"gpt-4o-mini-2024-07-18","batch":true}]}'),
('{"object":"bucket","start_time":1739923200,"end_time":1740009600,"results":[{"object":"organization.usage.completions.result","input_tokens":312800,"output_tokens":156200,"num_model_requests":3780,"input_cached_tokens":95400,"project_id":"proj_customer_support","user_id":null,"api_key_id":"key_prod_001","model":"gpt-4o-2024-08-06","batch":false},{"object":"organization.usage.completions.result","input_tokens":105600,"output_tokens":48900,"num_model_requests":1380,"input_cached_tokens":14200,"project_id":"proj_customer_support","user_id":null,"api_key_id":"key_prod_001","model":"gpt-4o-mini-2024-07-18","batch":true},{"object":"organization.usage.completions.result","input_tokens":48700,"output_tokens":26100,"num_model_requests":820,"input_cached_tokens":0,"project_id":"proj_internal_tools","user_id":"user_eng_01","api_key_id":"key_dev_002","model":"gpt-4o-2024-08-06","batch":false}]}'),
('{"object":"bucket","start_time":1740096000,"end_time":1740182400,"results":[{"object":"organization.usage.completions.result","input_tokens":345200,"output_tokens":172100,"num_model_requests":4120,"input_cached_tokens":102300,"project_id":"proj_customer_support","user_id":null,"api_key_id":"key_prod_001","model":"gpt-4o-2024-08-06","batch":false},{"object":"organization.usage.completions.result","input_tokens":125400,"output_tokens":58200,"num_model_requests":1620,"input_cached_tokens":22100,"project_id":"proj_customer_support","user_id":null,"api_key_id":"key_prod_001","model":"gpt-4o-mini-2024-07-18","batch":true},{"object":"organization.usage.completions.result","input_tokens":67800,"output_tokens":34200,"num_model_requests":1050,"input_cached_tokens":5200,"project_id":"proj_internal_tools","user_id":"user_eng_01","api_key_id":"key_dev_002","model":"gpt-4o-2024-08-06","batch":false}]}'),
('{"object":"bucket","start_time":1740182400,"end_time":1740268800,"results":[{"object":"organization.usage.completions.result","input_tokens":278900,"output_tokens":138400,"num_model_requests":3340,"input_cached_tokens":78500,"project_id":"proj_customer_support","user_id":null,"api_key_id":"key_prod_001","model":"gpt-4o-2024-08-06","batch":false},{"object":"organization.usage.completions.result","input_tokens":92100,"output_tokens":42800,"num_model_requests":1180,"input_cached_tokens":10800,"project_id":"proj_customer_support","user_id":null,"api_key_id":"key_prod_001","model":"gpt-4o-mini-2024-07-18","batch":true}]}');

-------------------------------------------------------------------------------
-- 4. APPROACH 1: Schema-on-Read (FLATTEN + Views)
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW V_COMPLETIONS
  COMMENT = 'DEMO: Approach 1 - Flattened chat completions, one row per choice (Expires: 2026-03-28)'
AS
SELECT
    raw:id::STRING                                          AS completion_id,
    raw:model::STRING                                       AS model,
    TO_TIMESTAMP(raw:created::NUMBER)                       AS created_at,
    raw:system_fingerprint::STRING                          AS system_fingerprint,
    c.value:index::NUMBER                                   AS choice_index,
    c.value:finish_reason::STRING                           AS finish_reason,
    c.value:message:role::STRING                            AS message_role,
    c.value:message:content::STRING                         AS content,
    c.value:message:refusal::STRING                         AS refusal,
    IFF(c.value:message:tool_calls IS NOT NULL, TRUE, FALSE)
                                                            AS has_tool_calls,
    ARRAY_SIZE(c.value:message:tool_calls)                  AS tool_call_count,
    raw:usage:prompt_tokens::NUMBER                         AS prompt_tokens,
    raw:usage:completion_tokens::NUMBER                     AS completion_tokens,
    raw:usage:total_tokens::NUMBER                          AS total_tokens,
    raw:usage:prompt_tokens_details:cached_tokens::NUMBER   AS cached_tokens,
    raw:usage:completion_tokens_details:reasoning_tokens::NUMBER
                                                            AS reasoning_tokens,
    loaded_at,
    source_file
FROM RAW_CHAT_COMPLETIONS,
     LATERAL FLATTEN(INPUT => raw:choices) c;

CREATE OR REPLACE VIEW V_TOOL_CALLS
  COMMENT = 'DEMO: Approach 1 - Flattened tool calls with parsed arguments (Expires: 2026-03-28)'
AS
SELECT
    raw:id::STRING                                          AS completion_id,
    raw:model::STRING                                       AS model,
    TO_TIMESTAMP(raw:created::NUMBER)                       AS created_at,
    c.value:index::NUMBER                                   AS choice_index,
    t.value:id::STRING                                      AS tool_call_id,
    t.value:type::STRING                                    AS tool_type,
    t.value:function:name::STRING                           AS function_name,
    t.value:function:arguments::STRING                      AS arguments_raw,
    TRY_PARSE_JSON(t.value:function:arguments::STRING)      AS arguments_parsed,
    raw:usage:prompt_tokens::NUMBER                         AS prompt_tokens,
    raw:usage:completion_tokens::NUMBER                     AS completion_tokens
FROM RAW_CHAT_COMPLETIONS,
     LATERAL FLATTEN(INPUT => raw:choices) c,
     LATERAL FLATTEN(INPUT => c.value:message:tool_calls, OUTER => TRUE) t
WHERE c.value:message:tool_calls IS NOT NULL;

CREATE OR REPLACE VIEW V_STRUCTURED_OUTPUTS
  COMMENT = 'DEMO: Approach 1 - Structured JSON outputs parsed from content (Expires: 2026-03-28)'
AS
SELECT
    raw:id::STRING                                          AS completion_id,
    raw:model::STRING                                       AS model,
    TO_TIMESTAMP(raw:created::NUMBER)                       AS created_at,
    c.value:message:content::STRING                         AS content_raw,
    TRY_PARSE_JSON(c.value:message:content::STRING)         AS content_parsed,
    raw:usage:total_tokens::NUMBER                          AS total_tokens
FROM RAW_CHAT_COMPLETIONS,
     LATERAL FLATTEN(INPUT => raw:choices) c
WHERE TRY_PARSE_JSON(c.value:message:content::STRING) IS NOT NULL
  AND c.value:message:refusal IS NULL;

CREATE OR REPLACE VIEW V_BATCH_RESULTS
  COMMENT = 'DEMO: Approach 1 - Unwrapped batch results with error handling (Expires: 2026-03-28)'
AS
SELECT
    raw:id::STRING                                          AS batch_request_id,
    raw:custom_id::STRING                                   AS custom_id,
    IFF(raw:error IS NOT NULL, 'ERROR', 'SUCCESS')          AS outcome,
    raw:error:code::STRING                                  AS error_code,
    raw:error:message::STRING                               AS error_message,
    raw:response:status_code::NUMBER                        AS http_status,
    raw:response:request_id::STRING                         AS request_id,
    raw:response:body:id::STRING                            AS completion_id,
    raw:response:body:model::STRING                         AS model,
    TO_TIMESTAMP(raw:response:body:created::NUMBER)         AS created_at,
    c.value:message:content::STRING                         AS content,
    c.value:message:refusal::STRING                         AS refusal,
    c.value:finish_reason::STRING                           AS finish_reason,
    raw:response:body:usage:prompt_tokens::NUMBER           AS prompt_tokens,
    raw:response:body:usage:completion_tokens::NUMBER       AS completion_tokens,
    raw:response:body:usage:total_tokens::NUMBER            AS total_tokens,
    loaded_at
FROM RAW_BATCH_OUTPUTS,
     LATERAL FLATTEN(INPUT => raw:response:body:choices, OUTER => TRUE) c;

CREATE OR REPLACE VIEW V_TOKEN_USAGE
  COMMENT = 'DEMO: Approach 1 - Flattened usage buckets for time-series analysis (Expires: 2026-03-28)'
AS
SELECT
    TO_TIMESTAMP(raw:start_time::NUMBER)                    AS bucket_start,
    TO_TIMESTAMP(raw:end_time::NUMBER)                      AS bucket_end,
    r.value:model::STRING                                   AS model,
    r.value:project_id::STRING                              AS project_id,
    r.value:user_id::STRING                                 AS user_id,
    r.value:api_key_id::STRING                              AS api_key_id,
    r.value:batch::BOOLEAN                                  AS is_batch,
    r.value:input_tokens::NUMBER                            AS input_tokens,
    r.value:output_tokens::NUMBER                           AS output_tokens,
    r.value:input_cached_tokens::NUMBER                     AS cached_tokens,
    r.value:num_model_requests::NUMBER                      AS request_count,
    r.value:input_tokens::NUMBER + r.value:output_tokens::NUMBER
                                                            AS total_tokens,
    loaded_at
FROM RAW_USAGE_BUCKETS,
     LATERAL FLATTEN(INPUT => raw:results) r;

-------------------------------------------------------------------------------
-- 5. APPROACH 2: Medallion Architecture (Dynamic Tables)
-------------------------------------------------------------------------------

-- Silver: Typed completions
CREATE OR REPLACE DYNAMIC TABLE DT_COMPLETIONS
  TARGET_LAG = '5 minutes'
  WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
  COMMENT = 'DEMO: Approach 2 Silver - typed chat completions (Expires: 2026-03-28)'
AS
SELECT
    raw:id::STRING                                              AS completion_id,
    raw:model::STRING                                           AS model,
    TO_TIMESTAMP(raw:created::NUMBER)                           AS created_at,
    raw:system_fingerprint::STRING                              AS system_fingerprint,
    c.value:index::NUMBER                                       AS choice_index,
    c.value:finish_reason::STRING                               AS finish_reason,
    c.value:message:role::STRING                                AS message_role,
    c.value:message:content::STRING                             AS content,
    LENGTH(c.value:message:content::STRING)                     AS content_length,
    c.value:message:refusal::STRING                             AS refusal,
    IFF(c.value:message:refusal IS NOT NULL, TRUE, FALSE)       AS is_refusal,
    IFF(c.value:message:tool_calls IS NOT NULL, TRUE, FALSE)    AS has_tool_calls,
    COALESCE(ARRAY_SIZE(c.value:message:tool_calls), 0)         AS tool_call_count,
    IFF(TRY_PARSE_JSON(c.value:message:content::STRING) IS NOT NULL
        AND c.value:message:refusal IS NULL, TRUE, FALSE)       AS is_structured_output,
    raw:usage:prompt_tokens::NUMBER                             AS prompt_tokens,
    raw:usage:completion_tokens::NUMBER                         AS completion_tokens,
    raw:usage:total_tokens::NUMBER                              AS total_tokens,
    raw:usage:prompt_tokens_details:cached_tokens::NUMBER       AS cached_tokens,
    raw:usage:completion_tokens_details:reasoning_tokens::NUMBER AS reasoning_tokens,
    ROUND(raw:usage:prompt_tokens_details:cached_tokens::NUMBER /
          NULLIF(raw:usage:prompt_tokens::NUMBER, 0), 3)        AS cache_hit_ratio,
    loaded_at,
    source_file
FROM RAW_CHAT_COMPLETIONS,
     LATERAL FLATTEN(INPUT => raw:choices) c;

-- Silver: Parsed tool calls
CREATE OR REPLACE DYNAMIC TABLE DT_TOOL_CALLS
  TARGET_LAG = '5 minutes'
  WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
  COMMENT = 'DEMO: Approach 2 Silver - parsed tool calls (Expires: 2026-03-28)'
AS
SELECT
    raw:id::STRING                                              AS completion_id,
    raw:model::STRING                                           AS model,
    TO_TIMESTAMP(raw:created::NUMBER)                           AS created_at,
    c.value:index::NUMBER                                       AS choice_index,
    t.index                                                     AS tool_call_index,
    t.value:id::STRING                                          AS tool_call_id,
    t.value:type::STRING                                        AS tool_type,
    t.value:function:name::STRING                               AS function_name,
    t.value:function:arguments::STRING                          AS arguments_json,
    TRY_PARSE_JSON(t.value:function:arguments::STRING)          AS arguments_parsed,
    ARRAY_SIZE(OBJECT_KEYS(
        TRY_PARSE_JSON(t.value:function:arguments::STRING)
    ))                                                          AS argument_count,
    raw:usage:prompt_tokens::NUMBER                             AS prompt_tokens,
    raw:usage:completion_tokens::NUMBER                         AS completion_tokens,
    loaded_at
FROM RAW_CHAT_COMPLETIONS,
     LATERAL FLATTEN(INPUT => raw:choices) c,
     LATERAL FLATTEN(INPUT => c.value:message:tool_calls) t;

-- Silver: Batch outcomes
CREATE OR REPLACE DYNAMIC TABLE DT_BATCH_OUTCOMES
  TARGET_LAG = '5 minutes'
  WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
  COMMENT = 'DEMO: Approach 2 Silver - batch outcomes with error handling (Expires: 2026-03-28)'
AS
SELECT
    raw:id::STRING                                              AS batch_request_id,
    raw:custom_id::STRING                                       AS custom_id,
    CASE
        WHEN raw:error IS NOT NULL                    THEN 'API_ERROR'
        WHEN c.value:message:refusal IS NOT NULL      THEN 'REFUSAL'
        WHEN raw:response:status_code::NUMBER != 200  THEN 'HTTP_ERROR'
        ELSE 'SUCCESS'
    END                                                         AS outcome,
    raw:error:code::STRING                                      AS error_code,
    raw:error:message::STRING                                   AS error_message,
    raw:response:status_code::NUMBER                            AS http_status,
    raw:response:body:model::STRING                             AS model,
    c.value:message:content::STRING                             AS content,
    TRY_PARSE_JSON(c.value:message:content::STRING)             AS content_parsed,
    c.value:message:refusal::STRING                             AS refusal,
    c.value:finish_reason::STRING                               AS finish_reason,
    COALESCE(raw:response:body:usage:total_tokens::NUMBER, 0)   AS total_tokens,
    loaded_at
FROM RAW_BATCH_OUTPUTS,
     LATERAL FLATTEN(INPUT => raw:response:body:choices, OUTER => TRUE) c;

-- Silver: Flattened usage
CREATE OR REPLACE DYNAMIC TABLE DT_USAGE_FLAT
  TARGET_LAG = '5 minutes'
  WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
  COMMENT = 'DEMO: Approach 2 Silver - flattened usage records (Expires: 2026-03-28)'
AS
SELECT
    TO_TIMESTAMP(raw:start_time::NUMBER)                        AS bucket_start,
    TO_TIMESTAMP(raw:end_time::NUMBER)                          AS bucket_end,
    DATE_TRUNC('day', TO_TIMESTAMP(raw:start_time::NUMBER))     AS bucket_date,
    r.value:model::STRING                                       AS model,
    r.value:project_id::STRING                                  AS project_id,
    r.value:user_id::STRING                                     AS user_id,
    r.value:api_key_id::STRING                                  AS api_key_id,
    r.value:batch::BOOLEAN                                      AS is_batch,
    r.value:input_tokens::NUMBER                                AS input_tokens,
    r.value:output_tokens::NUMBER                               AS output_tokens,
    COALESCE(r.value:input_cached_tokens::NUMBER, 0)            AS cached_tokens,
    r.value:num_model_requests::NUMBER                          AS request_count,
    loaded_at
FROM RAW_USAGE_BUCKETS,
     LATERAL FLATTEN(INPUT => raw:results) r;

-- Gold: Daily cost estimation
CREATE OR REPLACE DYNAMIC TABLE DT_DAILY_TOKEN_SUMMARY
  TARGET_LAG = '5 minutes'
  WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
  COMMENT = 'DEMO: Approach 2 Gold - daily token spend estimates (Expires: 2026-03-28)'
AS
SELECT
    bucket_date,
    model,
    project_id,
    is_batch,
    SUM(input_tokens)                                           AS total_input_tokens,
    SUM(output_tokens)                                          AS total_output_tokens,
    SUM(cached_tokens)                                          AS total_cached_tokens,
    SUM(request_count)                                          AS total_requests,
    SUM(input_tokens) + SUM(output_tokens)                      AS total_tokens,
    ROUND(SUM(input_tokens)  * IFF(model ILIKE '%mini%', 0.15, 2.50) / 1e6, 4)
                                                                AS est_input_cost_usd,
    ROUND(SUM(output_tokens) * IFF(model ILIKE '%mini%', 0.60, 10.0) / 1e6, 4)
                                                                AS est_output_cost_usd,
    ROUND(SUM(input_tokens)  * IFF(model ILIKE '%mini%', 0.15, 2.50) / 1e6
        + SUM(output_tokens) * IFF(model ILIKE '%mini%', 0.60, 10.0) / 1e6, 4)
                                                                AS est_total_cost_usd,
    ROUND(SUM(cached_tokens) / NULLIF(SUM(input_tokens), 0), 3)
                                                                AS overall_cache_hit_ratio,
    ROUND(SUM(input_tokens + output_tokens) / NULLIF(SUM(request_count), 0), 0)
                                                                AS avg_tokens_per_request
FROM DT_USAGE_FLAT
GROUP BY bucket_date, model, project_id, is_batch;

-- Gold: Tool call analytics
CREATE OR REPLACE DYNAMIC TABLE DT_TOOL_CALL_ANALYTICS
  TARGET_LAG = '5 minutes'
  WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
  COMMENT = 'DEMO: Approach 2 Gold - tool call analytics (Expires: 2026-03-28)'
AS
SELECT
    function_name,
    model,
    COUNT(*)                                                    AS invocation_count,
    COUNT(DISTINCT completion_id)                               AS unique_completions,
    AVG(argument_count)                                         AS avg_argument_count,
    SUM(prompt_tokens)                                          AS total_prompt_tokens,
    SUM(completion_tokens)                                      AS total_completion_tokens,
    ROUND(AVG(completion_tokens), 1)                            AS avg_tokens_per_call,
    MIN(created_at)                                             AS first_seen,
    MAX(created_at)                                             AS last_seen
FROM DT_TOOL_CALLS
GROUP BY function_name, model;

-- Gold: Batch health summary
CREATE OR REPLACE DYNAMIC TABLE DT_BATCH_SUMMARY
  TARGET_LAG = '5 minutes'
  WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
  COMMENT = 'DEMO: Approach 2 Gold - batch health summary (Expires: 2026-03-28)'
AS
SELECT
    outcome,
    error_code,
    model,
    COUNT(*)                                                    AS record_count,
    ROUND(COUNT(*) * 100.0 /
          SUM(COUNT(*)) OVER (), 1)                             AS pct_of_total,
    SUM(total_tokens)                                           AS total_tokens_used,
    ROUND(AVG(total_tokens), 0)                                 AS avg_tokens_per_record
FROM DT_BATCH_OUTCOMES
GROUP BY outcome, error_code, model;

-------------------------------------------------------------------------------
-- 6. APPROACH 3: Cortex AI Enrichment (requires Cortex availability)
--    Uncomment to deploy. Uses Cortex LLM functions which consume credits.
-------------------------------------------------------------------------------

-- CREATE OR REPLACE DYNAMIC TABLE DT_ENRICHED_COMPLETIONS
--   TARGET_LAG = '10 minutes'
--   WAREHOUSE = SFE_OPENAI_DATA_ENG_WH
--   COMMENT = 'DEMO: Approach 3 - Cortex-enriched completions (Expires: 2026-03-28)'
-- AS
-- SELECT
--     completion_id, model, created_at, finish_reason, content,
--     content_length, is_refusal, has_tool_calls, is_structured_output,
--     prompt_tokens, completion_tokens, total_tokens,
--     SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
--         content,
--         ['technical_explanation', 'data_analysis', 'code_generation',
--          'summarization', 'general_knowledge', 'recommendation']
--     ):label::STRING                                     AS topic_classification,
--     SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
--         content,
--         ['technical_explanation', 'data_analysis', 'code_generation',
--          'summarization', 'general_knowledge', 'recommendation']
--     ):score::FLOAT                                      AS topic_confidence,
--     SNOWFLAKE.CORTEX.SENTIMENT(content)                 AS sentiment_score,
--     CASE WHEN content_length > 200
--          THEN SNOWFLAKE.CORTEX.SUMMARIZE(content)
--          ELSE content END                               AS content_summary
-- FROM DT_COMPLETIONS
-- WHERE content IS NOT NULL AND is_refusal = FALSE;

-------------------------------------------------------------------------------
-- DONE
-------------------------------------------------------------------------------

SELECT 'Deployment complete!' AS status,
       CURRENT_TIMESTAMP() AS completed_at,
       'Approaches 1 & 2 active. Uncomment Approach 3 for Cortex enrichment.' AS note;
