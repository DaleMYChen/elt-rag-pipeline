'''
create a Snowflake connection at startup 
→ pass it into search_books  
→ llm.generate_answer
'''

import os
from contextlib import asynccontextmanager
from pydantic import BaseModel
from snowflake.snowpark import Session
from fastapi import FastAPI

from retriever import search_books
from llm import generate_answer

# A module-level connection placeholder
_session: Session | None = None

# Lifespan: create connection once on startup, close on shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _session
    _session = Session.builder.configs({
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database":  "BOOKS_WAREHOUSE",
    }).create()
    yield
    _session.close()

app = FastAPI(lifespan=lifespan)


### 1. Define pydantic dataclasses to validate
class QueryRequest(BaseModel):
    question: str

class BookResult(BaseModel):
    book_title: str
    category_name: str
    price_gbp: float
    rating_stars: int
    description: str | None

class AnswerResponse(BaseModel):
    answer: str
    books: list[BookResult]


### 2. Define routes

# ask/ will involve retrieval + response
@app.post("/ask", response_model=AnswerResponse)
def ask(req: QueryRequest):
    books = search_books(_session, req.question)
    if not books:
        return AnswerResponse(answer="No matching books found.", books=[])
    answer = generate_answer(_session, req.question, books)
    return AnswerResponse(answer=answer, books=books)



# @app.get("/debug")
# def debug():
#     """Diagnostic endpoint — remove before production."""
#     results = {}
#     try:
#         # 1. Confirm session identity
#         row = _session.sql("SELECT CURRENT_ROLE(), CURRENT_USER(), CURRENT_WAREHOUSE()").collect()[0]
#         results["session"] = {"role": row[0], "user": row[1], "warehouse": row[2]}
#     except Exception as e:
#         results["session"] = str(e)

#     try:
#         # 2. Test Cortex Search via SQL (SEARCH_PREVIEW)
#         row = _session.sql(
#             "SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW("
#             "'BOOKS_WAREHOUSE.MARTS.BOOK_SEARCH',"
#             "'{\"query\":\"test\",\"columns\":[\"book_title\"],\"limit\":1}')"
#         ).collect()[0]
#         results["search_preview_sql"] = "OK"
#     except Exception as e:
#         results["search_preview_sql"] = str(e)

#     try:
#         # 3. Test Cortex Search via SDK (Root)
#         from snowflake.core import Root
#         svc = Root(_session).databases["BOOKS_WAREHOUSE"].schemas["MARTS"].cortex_search_services["BOOK_SEARCH"]
#         r = svc.search(query="test", columns=["book_title"], limit=1)
#         results["search_sdk"] = f"OK — {len(r.results)} result(s)"
#     except Exception as e:
#         results["search_sdk"] = str(e)

#     try:
#         # 4. Test Cortex Complete via SQL
#         row = _session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b','Say hi in one word')").collect()[0]
#         results["cortex_complete"] = row[0]
#     except Exception as e:
#         results["cortex_complete"] = str(e)

#     return results
