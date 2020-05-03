from __future__ import annotations
import os

from elasticsearch import Elasticsearch

ES_CLIENT_FQDN = os.getenv("ES_CLIENT_FQDN")
assert ES_CLIENT_FQDN is not None

ES_CLIENT_PORT = os.getenv("ES_CLIENT_PORT", 9200)

ES_USERNAME = os.getenv("ES_USERNAME")
assert ES_USERNAME is not None

ES_PASSWORD = os.getenv("ES_PASSWORD")
assert ES_PASSWORD is not None

ES_CA_CERTS = os.getenv("ES_CA_CERTS")
assert ES_CA_CERTS is not None

def get_client() -> Elasticsearch:
    return Elasticsearch(
        f"https://{ES_CLIENT_FQDN}:{ES_CLIENT_PORT}",
        http_auth=(ES_USERNAME, ES_PASSWORD),
        use_ssl=True,
        verify_certs=True,
        ca_certs=ES_CA_CERTS,
    )
