import argparse
import functools
import json
import logging
import multiprocessing
import tqdm

import reindexer


def main():
    args = parse_args()

    logging.basicConfig(
        filename='fast_elasticsearch_reindex.log',
        level=logging.INFO,
        format='[%(asctime)s|%(name)s|%(levelname).4s] [PID %(process)d] %(message)s',
    )

    start(
        args=args,
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
        default=['127.0.0.1:9201'],
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


def start(
    args,
):
    total = reindexer.count(
        host=args.src_hosts,
        indices=args.indices,
        query=args.query,
    )

    if not total:
        print('No documents were found, exiting.')

        return

    options = reindexer.Options(
        src_hosts=args.src_hosts,
        dest_hosts=args.dest_hosts,
        indices=args.indices,
        query=args.query,
        workers=args.workers,
        slice_field=args.slice_field,
        size=args.size,
        scroll=args.scroll,
    )

    manager = multiprocessing.Manager()
    progress_queue = manager.Queue()

    progress_process = multiprocessing.Process(
        target=update_progress_bar,
        args=(
            progress_queue,
            total,
        ),
    )

    progress_process.start()

    with multiprocessing.Pool(args.workers) as pool:
        logging.info(f'Starting {args.workers} workers')

        func = functools.partial(
            reindex,
            options=options,
            progress_queue=progress_queue,
        )

        pool.map(
            func,
            range(args.workers),
        )

    logging.info('DONE.')


def reindex(
    worker_id: int,
    options: reindexer.Options,
    progress_queue,
):
    reindexer_instance = reindexer.Reindexer(
        worker_id=worker_id,
        options=options,
        progress_queue=progress_queue,
    )

    for index in options.indices:
        reindexer_instance.reindex(
            index=index,
        )


def update_progress_bar(
    queue,
    total,
):
    progress_bar = tqdm.tqdm(
        unit='doc',
        mininterval=0.5,
        total=total,
    )

    created = 0
    errors = 0
    skipped = 0

    while True:
        try:
            stats = queue.get()
        except EOFError:
            return

        created += stats.created
        errors += stats.errors
        skipped += stats.skipped
        total = stats.created + stats.errors + stats.skipped

        progress_bar.update(total)

        progress_bar.set_postfix(
            created=created,
            errors=errors,
            skipped=skipped,
        )


if __name__ == '__main__':
    main()
