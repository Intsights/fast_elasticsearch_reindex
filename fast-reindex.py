import argparse
import asyncio
import ciso8601
import datetime
import elasticsearch
import elasticsearch_async
import functools
import itertools
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

    if total:
        logging.info(f'Found {total} documents')
    else:
        logging.error('No documents were found, exiting.')
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

    with multiprocessing.Pool(args.max_workers) as pool:
        slices = list(
            itertools.zip_longest(
                *(iter(range(args.max_slices)),) * int(args.max_slices/args.max_workers)
            ),
        )

        print(f'Slice Distribution: {slices}')

        func = functools.partial(
            reindex,
            query=args.query,
            max_slices=args.max_slices,
            src_hosts=args.src_hosts,
            dest_hosts=args.dest_hosts,
            indices=args.indices,
            size=args.size,
            scroll=args.scroll,
            tqdm_queue=tqdm_queue,
        )

        logging.info(f'Starting {len(slices)} workers')

        pool.map(
            func,
            slices,
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
        serializer=ORJsonSerializer(),
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
    slices,
    query,
    max_slices,
    src_hosts,
    dest_hosts,
    indices,
    size,
    scroll,
    tqdm_queue,
):
    reindexer = Reindexer(
        query=query,
        src_hosts=src_hosts,
        dest_hosts=dest_hosts,
        indices=indices,
        slices=slices,
        max_slices=max_slices,
        size=size,
        scroll=scroll,
        tqdm_queue=tqdm_queue,
    )

    try:
        asyncio.get_event_loop().run_until_complete(reindexer.start())
    finally:
        asyncio.get_event_loop().run_until_complete(reindexer.close())


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--src-hosts',
        nargs='*',
        default=[
        ],
    )

    parser.add_argument(
        '--dest-hosts',
        nargs='*',
        default=[
        ],
    )

    parser.add_argument(
        '--query',
        default='{"query":{"range":{"created_date":{"gte":"now-1w", "lte": "now"}}}}',
    )

    parser.add_argument(
        '--max-slices',
        type=int,
        default=8,
    )

    parser.add_argument(
        '--max-workers',
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
        '--indices',
        nargs='*',
        default=[
            'domains',
            'subdomains',
        ],
    )

    args = parser.parse_args()

    args.query = json.loads(args.query)


    if args.max_slices < args.max_workers:
        raise InvalidArg('Max slices has to be greater than or equals to max workers')

    return args


class Reindexer:
    def __init__(
        self,
        query,
        src_hosts,
        dest_hosts,
        indices,
        max_slices,
        size,
        scroll,
        slices=None,
        tqdm_queue=None,
    ):
        self.src_client = elasticsearch_async.AsyncElasticsearch(
            hosts=src_hosts,
            timeout=120,
            dead_timeout=0,
            timeout_cutoff=0,
            serializer=ORJsonSerializer(),
        )

        self.dest_client = elasticsearch_async.AsyncElasticsearch(
            hosts=dest_hosts,
            timeout=120,
            dead_timeout=0,
            timeout_cutoff=0,
            serializer=ORJsonSerializer(),
        )

        self.query = query
        self.indices = indices
        self.slices = slices
        self.max_slices = max_slices
        self.size = size
        self.scroll = scroll
        self.tqdm_queue = tqdm_queue

    async def close(
        self,
    ):
        await self.dest_client.transport.close()
        await self.src_client.transport.close()

    async def start(
        self,
    ):
        logging.info('Starting worker')

        tasks = []

        loop = asyncio.get_event_loop()

        for index in self.indices:
            tasks += [
                self.reindex(
                    index=index,
                    slice_id=i,
                ) for i in self.slices
            ]

        await asyncio.gather(*tasks)

        logging.info('Worker done')

    async def reindex(
        self,
        index,
        slice_id,
    ):
        logging.info(f'Indexing {index} (slice ID {slice_id})')

        if self.max_slices > 1:
            _slice = {
                'slice': {
                    'field': 'created_date',
                    'id': slice_id,
                    'max': self.max_slices,
                },
            }
        else:
            _slice = {}

        for attempt in tenacity.Retrying(
            wait=tenacity.wait_exponential(multiplier=1, min=1, max=3),
            before_sleep=tenacity.before_sleep_log(tenacity_logger, logging.WARN)
        ):
            with attempt:
                scroll_response = await self.src_client.search(
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
                            '_index': index,
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
                    bulk_response = await self.dest_client.bulk(
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

                await asyncio.sleep(0.250)

            if retry_queue:
                queue = retry_queue

                continue

            for attempt in tenacity.Retrying(
                wait=tenacity.wait_exponential(multiplier=1, min=1, max=3),
                before_sleep=tenacity.before_sleep_log(tenacity_logger, logging.WARN)
            ):
                with attempt:
                    scroll_response = await self.src_client.scroll(
                        body={
                            'scroll_id': scroll_response['_scroll_id'],
                            'scroll': self.scroll,
                        },
                    )

            queue = scroll_response['hits']['hits']

        logging.info(f'Done indexing {index} (slice ID {slice_id})')


class InvalidArg(Exception): pass


if __name__ == '__main__':
    main()
