# Fast Elasticsearch Reindex

A multiprocessing based alternative to Elasticsearch Reindex API.

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

For local environment, install the Python package with `pip install .`,
alternatively you can use Docker:
```
$ docker build -t fast_elasticsearch_reindex .
$ docker run fast_elasticsearch_reindex --help
```
