# %%

import boto3
import json
import requests
from requests_aws4auth import AWS4Auth
import pandas as pd
from typing import List

# %%

region = 'us-west-2'  # For example, us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                   region, service, session_token=credentials.token)

# The OpenSearch domain endpoint with https://{domain}.{region}.es.amazonaws.com without trailing slash
host = 'https://<>.us-west-2.es.amazonaws.com'
index = 'wikipedia'
data_file = 'data/articles_aggregate.csv'

# %% Define Functions


def create_index(index: str) -> None:
    """
    Create Index
    """
    url = host + '/' + index
    query = {
        "settings": {
            "analysis": {
                "char_filter": {
                    "normalize": {
                        "type": "icu_normalizer",
                        "name": "nfkc",
                        "mode": "compose"
                    }
                },
                "tokenizer": {
                    "ja_kuromoji_tokenizer": {
                        "mode": "search",
                        "type": "kuromoji_tokenizer",
                        "discard_compound_token": True,
                        "user_dictionary_rules": [
                        ]
                    }
                },
                "analyzer": {
                    "ja_kuromoji_index_analyzer": {
                        "type": "custom",
                        "char_filter": [
                            "normalize"
                        ],
                        "tokenizer": "ja_kuromoji_tokenizer",
                        "filter": [
                            "kuromoji_baseform",
                            "kuromoji_part_of_speech",
                            "cjk_width",
                            "ja_stop",
                            "kuromoji_stemmer",
                            "lowercase"
                        ]
                    },
                    "ja_kuromoji_search_analyzer": {
                        "type": "custom",
                        "char_filter": [
                            "normalize"
                        ],
                        "tokenizer": "ja_kuromoji_tokenizer",
                        "filter": [
                            "kuromoji_baseform",
                            "kuromoji_part_of_speech",
                            "cjk_width",
                            "ja_stop",
                            "kuromoji_stemmer",
                            "lowercase"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "title": {
                    "type": "text"
                },
                "content": {
                    "type": "text",
                    "search_analyzer": "ja_kuromoji_search_analyzer",
                    "analyzer": "ja_kuromoji_index_analyzer",
                }
            }
        }
    }
    headers = {"Content-Type": "application/json"}
    r = requests.put(url, auth=awsauth, headers=headers,
                     data=json.dumps(query))
    print(r.text)


def delete_index(index: str) -> None:
    """
    Delete Index
    """
    url = host + '/' + index
    headers = {"Content-Type": "application/json"}
    r = requests.delete(url, auth=awsauth, headers=headers, data="")
    print(r.text)


def bulk(
        index: str,
        documents: List[object]
) -> None:
    """
    Bulk insert Data
    """
    url = host + '/_bulk/'
    payload = ""
    headers = {"Content-Type": "application/json"}
    for i, document in enumerate(documents):
        payload += json.dumps({"index": {"_index": index,
                              "_type": "_doc", "_id": i+1}}) + "\n"
        payload += json.dumps(document) + "\n"
        # 1000件ごとにアップロード
        if i % 1000 == 999:
            r = requests.post(url, auth=awsauth, headers=headers, data=payload)
            print(r.text[:100])
            payload = ""
    r = requests.post(url, auth=awsauth, headers=headers, data=payload)
    print(r.text[:100])


def add(
        index: str,
        document
) -> None:
    """
    Insert Single Item
    """
    url = host + '/' + index + '/_doc/?refresh'
    query = document
    headers = {"Content-Type": "application/json"}
    r = requests.post(url, auth=awsauth, headers=headers,
                      data=json.dumps(query))
    print(r.text)


def search(
    index: str,
    keyword: str,
    size: int = 20
) -> requests.Response:
    """
    Search Index
    """
    url = host + '/' + index + '/_search'
    query = {
        "query": {
            "multi_match": {
                "query": keyword,
                "fields": ["title^2", "content"],
            }
        },
        "size": size,
        "highlight": {
            "fields": {
                "content": {
                }
            }
        }
    }
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers,
                     data=json.dumps(query))
    # print(r.text)
    return r


def more_like_this(
    index: str,
    doc_id: str,
    size: int = 20
) -> requests.Response:
    """
    Search Index
    """
    url = host + '/' + index + '/_search'
    query = {
        "query": {
            "more_like_this": {
                "fields": ["title", "content"],
                "like": [
                    {
                        "_index": index,
                        "_id": doc_id
                    }
                ],
                "min_term_freq": 1,
                "max_query_terms": 12
            }
        },
        "size": size
    }
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers,
                     data=json.dumps(query))
    # print(r.text)
    return r


def get_records(index: str) -> requests.Response:
    """
    List Items
    """
    url = host + '/' + index + '/_search'
    query = {
        "size": "50",
        "query": {
            "match_all": {}
        }
    }
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers,
                     data=json.dumps(query))
    result = json.loads(r.text)
    print(result)
    return r


def index_stats(index: str) -> requests.Response:
    """
    Index Stats
    """
    url = host + '/' + index + '/_stats'
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers)
    result = json.loads(r.text)
    print(result)
    return r


def analyze(
    index: str,
    text: str
) -> requests.Response:
    """
    Test Analyzer Output (for debug and etc.)
    """
    url = host + '/' + index + '/_analyze'
    query = {
        "analyzer": "ja_kuromoji_index_analyzer",
        "text": text
    }
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers,
                     data=json.dumps(query))
    print(r.text)
    return r


def search_analyze(
    index: str,
    text: str
) -> requests.Response:
    """
    Test Search Analyzer Output (for debug and etc.)
    """
    url = host + '/' + index + '/_analyze'
    query = {
        "analyzer": "ja_kuromoji_search_analyzer",
        "text": text
    }
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers,
                     data=json.dumps(query))
    print(r.text)
    return r


# %% Create Index

create_index(index)

# %% Load Data

df = pd.read_csv(data_file)
movie_list = df.to_dict('records')
# print(json.dumps(movie_list[0], ensure_ascii=False))

# %% Dump Data

bulk(index, movie_list)

# %%

# get_records(index)
index_stats(index)

# %%

r = search(index, "アベンジャーズ")
result = json.loads(r.text)
# print(result['hits']['hits'][0])
# print(str(result['hits'])[:200])
print(result['hits']['total'])
for item in result['hits']['hits']:
    print({"title": item['_source']['title'],
          "id": item['_id'], "score": item['_score']})

# %%

r = more_like_this(index, "2593")
result = json.loads(r.text)
print(result['hits']['total'])
for item in result['hits']['hits']:
    print({"title": item['_source']['title'],
          "id": item['_id'], "score": item['_score']})

# %%

# delete_index(index)


# %%
