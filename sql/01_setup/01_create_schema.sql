/*==============================================================================
SETUP - OpenAI Data Engineering
Creates schema, warehouse, and session context.
==============================================================================*/

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
