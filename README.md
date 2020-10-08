# Fast Elasticsearch Reindex

A Python based alternative to Elasticsearch Reindex API with multiprocessing
support. Since Elasticsearch Reindex API doesn't support slicing when reindexing
from a remote cluster, the entire process can take many hours or even days,
depending on the cluster size. Based on [Sliced
Scroll](https://www.elastic.co/guide/en/elasticsearch/reference/master/paginate-search-results.html#slice-scroll)
and [Bulk
requests](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html),
this utility can be used as a faster alternative.

![fast_elasticsearch_reindex](https://i.ibb.co/Z6QKybN/fast-elasticsearch-reindex.gif)

## Usage

```
usage: fast_elasticsearch_reindex [-h] [--src-hosts [SRC_HOSTS [SRC_HOSTS ...]]] [--dest-hosts [DEST_HOSTS [DEST_HOSTS ...]]]
                                  [--query QUERY] [--workers WORKERS] [--size SIZE] [--scroll SCROLL] [--slice-field SLICE_FIELD]
                                  [--indices [INDICES [INDICES ...]]]

optional arguments:
  -h, --help            show this help message and exit
  --src-hosts [SRC_HOSTS [SRC_HOSTS ...]]
                        Source Elasticsearch hosts to reindex from (default: ['127.0.0.1:9200'])
  --dest-hosts [DEST_HOSTS [DEST_HOSTS ...]]
                        Destination Elasticsearch hosts to reindex to (default: ['127.0.0.1:9201'])
  --query QUERY         Search query (default: {})
  --workers WORKERS     Number of parallel workers (default: 8)
  --size SIZE           Search request size (default: 2000)
  --scroll SCROLL       Scroll request duration (default: 5m)
  --slice-field SLICE_FIELD
                        Field to slice by (default: None)
  --indices [INDICES [INDICES ...]]
                        Indices to reindex (default: None)
```

## Installation

Pip:
```
pip install fast_elasticsearch_reindex
```

Local:
```
$ pip install .
$ python -m fast_elasticsearch_reindex --help
```

Docker:
```
$ docker build -t fast_elasticsearch_reindex .
$ docker run fast_elasticsearch_reindex --help
```
