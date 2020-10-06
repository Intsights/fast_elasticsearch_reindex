import argparse
import datetime
import functools
import json
import logging
import multiprocessing
import tqdm

import reindexer


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
    total = reindexer.count(
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
    reindexer_instance = reindexer.Reindexer(
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
        reindexer_instance.reindex(
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


class InvalidArg(Exception): pass


if __name__ == '__main__':
    main()
