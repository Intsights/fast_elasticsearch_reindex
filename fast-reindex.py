import argparse
import datetime
import time
import elasticsearch
import functools
import json
import logging
import multiprocessing
import orjson
import tenacity
import tqdm


tenacity_logger = logging.getLogger('tenacity')


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


def main():
    try:
        args = parse_args()
    except InvalidArg as exception:
        print(f'Error: {exception}')
        return

    logging.basicConfig(
        filename='fast-reindex.log',
        level=logging.INFO,
        format='[%(asctime)s|%(name)s|%(levelname).4s] [PID %(process)d] %(message)s',
    )

    start(
        args=args,
    )


def start(
    args,
):
    total = count(
        host=args.src_hosts,
        indices=args.indices,
        query=args.query,
    )

    if not total:
        print('No documents were found, exiting.')

        return

    manager = multiprocessing.Manager()
    tqdm_queue = manager.Queue()

    progress_process = multiprocessing.Process(
        target=progress_queue,
        args=(
            tqdm_queue,
            total,
        ),
    )

    progress_process.start()

    with multiprocessing.Pool(args.workers) as pool:
        logging.info(f'Starting {args.workers} workers')

        func = functools.partial(
            reindex,
            workers=args.workers,
            query=args.query,
            slice_field=args.slice_field,
            src_hosts=args.src_hosts,
            dest_hosts=args.dest_hosts,
            indices=args.indices,
            size=args.size,
            scroll=args.scroll,
            tqdm_queue=tqdm_queue,
        )

        pool.map(
            func,
            range(args.workers),
        )

    logging.info('DONE.')


def count(
    host,
    indices,
    query,
):
    logging.info(f'Counting total documents of {indices} with {query}')

    client = elasticsearch.Elasticsearch(
        hosts=host,
        timeout=120,
    )

    return client.count(
        index=indices,
        body=query,
    )['count']


def progress_queue(
    queue,
    total,
):
    last_speed_log = datetime.datetime.now()
    created = 0
    errors = 0
    skipped = 0

    progress_bar = tqdm.tqdm(
        unit='doc',
        mininterval=0.5,
        total=total,
    )

    while True:
        try:
            stats = queue.get()
        except EOFError:
            return

        created += stats['created']
        errors += stats['errors']
        skipped += stats['skipped']
        total = stats['created'] + stats['errors'] + stats['skipped']

        progress_bar.update(total)

        progress_bar.set_postfix(
            created=created,
            errors=errors,
            skipped=skipped,
        )

        now = datetime.datetime.now()

        if now - last_speed_log >= datetime.timedelta(seconds=5):
            logging.info(progress_bar)

            last_speed_log = now


def reindex(
    worker_id,
    workers,
    query,
    slice_field,
    src_hosts,
    dest_hosts,
    indices,
    size,
    scroll,
    tqdm_queue,
):
    reindexer = Reindexer(
        worker_id=worker_id,
        workers=workers,
        query=query,
        slice_field=slice_field,
        src_hosts=src_hosts,
        dest_hosts=dest_hosts,
        indices=indices,
        size=size,
        scroll=scroll,
        tqdm_queue=tqdm_queue,
    )

    for index in indices:
        reindexer.reindex(
            index=index,
        )


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--src-hosts',
        nargs='*',
        default=['127.0.0.1:9200'],
    )

    parser.add_argument(
        '--dest-hosts',
        nargs='*',
        default=['127.0.0.1:9200'],
    )

    parser.add_argument(
        '--query',
        default='{}',
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=8,
    )

    parser.add_argument(
        '--size',
        type=int,
        default=2000,
    )

    parser.add_argument(
        '--scroll',
        default='5m',
    )

    parser.add_argument(
        '--slice-field',
    )

    parser.add_argument(
        '--indices',
        nargs='*',
    )

    args = parser.parse_args()

    args.query = json.loads(args.query)

    return args


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


class InvalidArg(Exception): pass


if __name__ == '__main__':
    main()
