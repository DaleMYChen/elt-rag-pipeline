-- ─────────────────────────────────────────────────────────────────────────────
-- Cortex Search Service — run once in the Snowflake console after dim_books
-- is populated. The service auto-refreshes within TARGET_LAG of any change
-- to dim_books, so no Airflow task is needed for re-indexing.
-- ─────────────────────────────────────────────────────────────────────────────


-- ── Step 1: Create the Cortex Search Service ─────────────────────────────────

CREATE OR REPLACE CORTEX SEARCH SERVICE BOOKS_WAREHOUSE.MARTS.BOOK_SEARCH

  -- ON: the single column Snowflake embeds for semantic search.
  -- We use 'search_text' — a concatenation of title, category and description
  -- — rather than description alone. Why? The embedding captures the full
  -- semantic context: a query like "Sherlock Holmes detective mystery" benefits
  -- from having the category "Mystery" and the title "A Study in Scarlet" in
  -- the indexed text, not just the description paragraph.
  ON search_text

  -- ATTRIBUTES: structured columns available for exact-match filtering in API
  -- calls (e.g. filter to only Mystery books, or books under £15).
  -- These are NOT embedded — they are returned as metadata alongside results.
  ATTRIBUTES book_title, category_name, price_gbp, rating_stars,
             stock_status, num_in_stock

  WAREHOUSE = COMPUTE_WH

  -- How stale the index is allowed to be relative to dim_books.
  -- '1 hour' means: after each dbt run updates dim_books, the search index
  -- will reflect the new data within one hour — no manual trigger needed.
  TARGET_LAG = '1 hour'

AS (
  -- This SELECT defines what gets indexed and what can be returned by queries.
  -- Cortex Search treats this like a view: it re-runs this query on each
  -- refresh cycle to detect new/changed rows.
  SELECT
    book_title,
    category_name,
    price_gbp,
    rating_stars,
    stock_status,
    num_in_stock,

    -- description is returned in API results so the LLM has the full text.
    -- It is NOT the column we embed (search_text is), but we include it here
    -- so we can pass it to the LLM prompt in retriever.py.
    description,

    -- search_text: what Snowflake actually converts to a vector embedding.
    -- Format: "Title [Category]: Description"
    -- Coalescing description to '' handles the NULLs that stg_inventory
    -- produces for books without a description section.
    book_title
      || ' ['  || COALESCE(category_name, '')  || ']: '
      || COALESCE(description, '')
    AS search_text

  FROM BOOKS_WAREHOUSE.MARTS.dim_books

  -- Exclude rows with no description — they carry no semantic signal.
  -- A book without a description would embed only its title and category,
  -- which is too little text for reliable semantic matching.
  WHERE description IS NOT NULL
);


-- ── Step 2: Grant privileges to the application role ─────────────────────────
-- Run as ACCOUNTADMIN. These are ONE-TIME setup grants — they persist until
-- explicitly revoked and do NOT need to be re-run on each deployment.
--
-- Why two separate grants?
--
-- Grant A — SNOWFLAKE.CORTEX_USER (database role in the shared SNOWFLAKE DB):
--   Gates access to ALL Cortex functions (COMPLETE, EMBED_TEXT, etc.) from
--   programmatic clients. The Snowflake console bypasses this for admin users,
--   which is why console queries work without it. Any application role that
--   calls Cortex functions via Python must have this grant.
--
-- Grant B — USAGE ON CORTEX SEARCH SERVICE (object-level privilege):
--   The search service is a named database object, not just a function. Like
--   a table or view, ownership does not automatically extend to other users
--   of the same role — it requires an explicit USAGE grant. Without this,
--   SEARCH_PREVIEW and the Python SDK both return 390404 even with SYSADMIN.
--
-- If you recreate the service (CREATE OR REPLACE), Grant A persists but
-- Grant B must be re-run because the object is replaced.

USE ROLE ACCOUNTADMIN;

-- Enables all Cortex functions (COMPLETE, SEARCH_PREVIEW, etc.) for SYSADMIN
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER
  TO ROLE SYSADMIN;

-- Enables access to this specific Cortex Search service for SYSADMIN
GRANT USAGE ON CORTEX SEARCH SERVICE BOOKS_WAREHOUSE.MARTS.BOOK_SEARCH
  TO ROLE SYSADMIN;


-- ── Step 3: Verify the service was created ────────────────────────────────────

SHOW CORTEX SEARCH SERVICES IN SCHEMA BOOKS_WAREHOUSE.MARTS;


-- ── Step 4: Smoke-test with a sample query ────────────────────────────────────
-- SEARCH_PREVIEW is the SQL interface to the service (useful for ad-hoc testing
-- in the Snowflake console). The Python SDK in retriever.py uses the same
-- service but through the snowflake-ml-python Root() client.
--
-- What is returned: the top-N *rows* from the SELECT above that are
-- semantically closest to the query string. Each result is one book — not a
-- sub-row chunk. The score reflects cosine similarity between the query
-- embedding and the search_text embedding.

SELECT
  PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'BOOKS_WAREHOUSE.MARTS.BOOK_SEARCH',
      '{
        "query": "a comforting story about family on a rainy day",
        "columns": ["book_title", "category_name", "price_gbp", "rating_stars", "description"],
        "limit": 3
      }'
    )
  ) AS raw_response;

-- Unwrap the results array from the JSON response:
SELECT
  value:book_title::VARCHAR    AS book_title,
  value:category_name::VARCHAR AS category_name,
  value:price_gbp::NUMBER      AS price_gbp,
  value:rating_stars::INTEGER  AS rating_stars,
  value:description::VARCHAR   AS description
FROM TABLE(
  FLATTEN(
    INPUT => PARSE_JSON(
      SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'BOOKS_WAREHOUSE.MARTS.BOOK_SEARCH',
        '{
          "query": "a comforting story about family on a rainy day",
          "columns": ["book_title", "category_name", "price_gbp", "rating_stars", "description"],
          "limit": 3
        }'
      )
    )['results']
  )
);
