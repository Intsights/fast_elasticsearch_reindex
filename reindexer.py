import elasticsearch
import logging
import orjson
import tenacity
import time


tenacity_logger = logging.getLogger('tenacity')

def count(
    host,
    indices,
    query,
):
    client = elasticsearch.Elasticsearch(
        hosts=host,
        timeout=120,
    )

    return client.count(
        index=indices,
        body=query,
    )['count']


class Reindexer:
    def __init__(
        self,
        workers,
        worker_id,
        slice_field,
        query,
        src_hosts,
        dest_hosts,
        indices,
        size,
        scroll,
        tqdm_queue=None,
    ):
        self.src_client = elasticsearch.Elasticsearch(
            hosts=src_hosts,
            timeout=120,
            dead_timeout=0,
            timeout_cutoff=0,
            serializer=ORJsonSerializer(),
        )

        self.dest_client = elasticsearch.Elasticsearch(
            hosts=dest_hosts,
            timeout=120,
            dead_timeout=0,
            timeout_cutoff=0,
            serializer=ORJsonSerializer(),
        )

        self.workers = workers
        self.worker_id = worker_id
        self.slice_field = slice_field
        self.query = query
        self.indices = indices
        self.size = size
        self.scroll = scroll
        self.tqdm_queue = tqdm_queue

    def __del__(
        self,
    ):
        self.dest_client.transport.close()
        self.src_client.transport.close()

    def reindex(
        self,
        index,
    ):
        logging.info(f'Indexing {index} (worker ID {self.worker_id})')

        if self.workers > 1:
            _slice = {
                'slice': {
                    'id': self.worker_id,
                    'max': self.workers,
                },
            }

            if self.slice_field:
                _slice['field'] = self.slice_field
        else:
            _slice = {}

        for attempt in tenacity.Retrying(
            wait=tenacity.wait_exponential(multiplier=1, min=1, max=3),
            before_sleep=tenacity.before_sleep_log(tenacity_logger, logging.WARN)
        ):
            with attempt:
                scroll_response = self.src_client.search(
                    index=index,
                    scroll=self.scroll,
                    size=self.size,
                    body={
                        **_slice,
                        **self.query,
                    },
                )

        queue = scroll_response['hits']['hits']

        while queue:
            created = 0
            skipped = 0
            errors = 0
            body = []

            for hit in queue:
                body += [
                    {
                        'create': {
                            '_index': 'my-dest-index',
                            '_id': hit['_id'],
                        },
                    },
                    hit['_source'],
                ]

            for attempt in tenacity.Retrying(
                wait=tenacity.wait_exponential(multiplier=1, min=1, max=3),
                before_sleep=tenacity.before_sleep_log(tenacity_logger, logging.WARN)
            ):
                with attempt:
                    bulk_response = self.dest_client.bulk(
                        body=body,
                    )

            too_many_requests = False

            retry_queue = []
            for i, item in enumerate(bulk_response['items']):
                if 'error' in item['create']:
                    if item['create']['status'] == 429:
                        too_many_requests = True

                        print('Too many requests', item, queue[i])
                        retry_queue.append(queue[i])
                    elif item['create']['error']['type'] == 'version_conflict_engine_exception':
                        skipped += 1
                    else:
                        logging.error(f'Failed to create: {item}')

                        errors += 1
                else:
                    created += 1

            self.tqdm_queue.put({
                'created': created,
                'skipped': skipped,
                'errors': errors,
            })

            if too_many_requests:
                logging.warning('Too many requests')

                time.sleep(0.250)

            if retry_queue:
                queue = retry_queue

                continue

            for attempt in tenacity.Retrying(
                wait=tenacity.wait_exponential(multiplier=1, min=1, max=3),
                before_sleep=tenacity.before_sleep_log(tenacity_logger, logging.WARN)
            ):
                with attempt:
                    scroll_response = self.src_client.scroll(
                        body={
                            'scroll_id': scroll_response['_scroll_id'],
                            'scroll': self.scroll,
                        },
                    )

            queue = scroll_response['hits']['hits']

        logging.info(f'Done indexing {index} (slice ID {self.worker_id})')


class ORJsonSerializer(
    elasticsearch.JSONSerializer,
):
    def dumps(
        self,
        data,
    ):
        if isinstance(data, (str, bytes)):
            return data

        return orjson.dumps(data).decode()

    def loads(
        self,
        data,
    ):
        return orjson.loads(data)
