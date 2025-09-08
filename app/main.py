from fastapi import FastAPI, Query
from opensearchpy import OpenSearch
from typing import List, Dict, Optional
import time
import random

app = FastAPI()
INDEX_NAME = "documents"
CONTENT_TYPES = ["article", "blog", "guide", "note"]

client = OpenSearch(
    hosts=[{"host": "opensearch-node1", "port": 9200}],
    use_ssl=False, verify_certs=False
)

def wait_for_opensearch(timeout_sec=60):
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            if client.ping():
                return True
        except Exception:
            pass
        time.sleep(1)
    return False

@app.on_event("startup")
def startup():
    wait_for_opensearch(60)
    if not client.indices.exists(index=INDEX_NAME):
        client.indices.create(
            index=INDEX_NAME,
            body={
                "mappings": {
                    "properties": {
                        "title": {"type": "text"},
                        "content": {"type": "text"},
                        "content_type": {"type": "keyword"}
                    }
                }
            }
        )
        sample_titles = [
            "Первый документ про Python",
            "Введение в OpenSearch",
            "Docker и контейнеры — кратко",
            "Как писать тесты",
            "Советы по разработке"
        ]
        sample_contents = [
            "Это небольшой тестовый контент, в котором упоминается Python и OpenSearch.",
            "В этом тексте рассказывается, как использовать OpenSearch для быстрого поиска.",
            "Docker помогает запускать приложения в контейнерах и упрощает деплой.",
            "Тестирование важно: пишите юнит-тесты и интеграционные тесты.",
            "Полезные советы по написанию чистого кода и рефакторингу."
        ]
        n = random.randint(3, 5)
        for i in range(n):
            doc = {
                "title": random.choice(sample_titles),
                "content": random.choice(sample_contents),
                "content_type": random.choice(CONTENT_TYPES)
            }
            client.index(index=INDEX_NAME, id=i+1, body=doc)
        client.indices.refresh(index=INDEX_NAME)

@app.get("/search")
def search(q: str, content_type: Optional[str] = Query(None)):
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"multi_match": {"query": q, "fields": ["title", "content"]}}
                ],
                "filter": []
            }
        }
    }
    if content_type:
        query_body["query"]["bool"]["filter"].append({"term": {"content_type": content_type}})
    res = client.search(index=INDEX_NAME, body=query_body)
    results: List[Dict] = []
    for hit in res["hits"]["hits"]:
        src = hit["_source"]
        results.append({"title": src.get("title"), "snippet": src.get("content", "")[:50]})
    return results
