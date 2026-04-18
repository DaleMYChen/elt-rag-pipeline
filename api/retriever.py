from __future__ import annotations
import json
import logging

from snowflake.snowpark import Session
from snowflake.core import Root

'''
Using Snowflake connection cursor to perform Cortex Search.
Requires:
    - Snowflake Connection;
    - Query;
    - Config: cortex service name, retrieval limit, metadata column names
'''

SEARCH_SERVICE = "BOOKS_WAREHOUSE.MARTS.BOOK_SEARCH"
RETURN_COLUMNS = ["book_title", "category_name", "price_gbp",
                  "rating_stars", "description", "stock_status", "num_in_stock"]

logger = logging.getLogger(__name__)

def search_books(session: Session, query: str, limit: int = 5) -> list[dict]:
    # Root() is the entry point to the snowflake-core SDK.
    # It navigates the object hierarchy: database → schema → service.
    # .search() handles embedding the query and returning ranked rows —
    # no SQL needed. This is the documented Python path for Cortex Search.
    svc = (
        Root(session)
        .databases["BOOKS_WAREHOUSE"]
        .schemas["MARTS"]
        .cortex_search_services["BOOK_SEARCH"]
    )
    result = svc.search(
        query=query,
        columns=RETURN_COLUMNS,
        limit=limit,
    )
    return result.results   # list of dicts, one per matched book
