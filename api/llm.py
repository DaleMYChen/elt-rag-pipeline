"""
call SNOWFLAKE.CORTEX.COMPLETE SQL → return answer string.
"""

from __future__ import annotations
import logging
import os
from typing import List, Optional
from snowflake.snowpark import Session
# snowflake.cortex.Complete lives in snowflake-ml-python (~500MB extra dependency).
# We avoid it by calling the same LLM via session.sql() — Snowpark sessions can
# execute any Snowflake SQL, including Cortex functions.

CORTEX_MODEL = os.environ.get("CORTEX_MODEL", "llama3.1-70b")
logger = logging.getLogger(__name__)

'''
retriever.search_books output type: list[dict] 

Retrieve necessary columns from Cortex Search results for prompt
'''

def _build_prompt(question: str, books: list[dict]) -> str:
    book_lines = []
    # use 1-indexing to specify books 1, 2, ...
    for i, b in enumerate(books, 1):
        book_lines.append(
            f"{i}. \"{b['book_title']}\" ({b['category_name']}, "
            f"£{b['price_gbp']}, {'★' * int(b['rating_stars'])})\n"
            f"   {b['description']}"
        )
    context = "\n\n".join(book_lines)

    return (
        "You are a book recommendation assistant.\n"
        "Answer using ONLY the books listed below. "
        "If none match the request, say so honestly — do not invent titles.\n\n"
        f"{context}\n\n"
        f"Question: {question}\nAnswer:"
    )



# ---------------------------------------------------------------------------
# Main Q&A function
# ---------------------------------------------------------------------------
def generate_answer(session: Session, question: str, books: list[dict]) -> str:
    prompt = _build_prompt(question, books)
    # Escape single quotes SQL-standard style (' → '') so apostrophes in
    # descriptions or the user's question don't break the string literal.
    prompt_safe = prompt.replace("'", "''")
    sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{CORTEX_MODEL}', '{prompt_safe}')"
    result = session.sql(sql).collect()
    return result[0][0]



