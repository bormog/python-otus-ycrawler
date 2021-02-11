import argparse
import asyncio
import logging
import os
import sys

from ycrawler import YCrawler


PAGE_LIMIT = 30
REPEAT_INTERVAL = 15
DRY_RUN = True
DOWNLOAD_DIR = 'pages'


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description='Crawler for https://news.ycombinator.com')
    arg_parser.add_argument('--repeat_interval', default=REPEAT_INTERVAL, type=int)
    arg_parser.add_argument('--page_limit', default=PAGE_LIMIT, type=int)
    arg_parser.add_argument('--download_dir', default=DOWNLOAD_DIR, type=str)
    arg_parser.add_argument('--dry_run', default=DRY_RUN, type=bool)
    arg_parser.add_argument('--logfile', default=None)
    arg_parser.add_argument('--loglevel', default='INFO', type=str)

    args = arg_parser.parse_args()

    logging.basicConfig(
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
        filename=args.logfile,
        level=getattr(logging, args.loglevel)
    )
    if not os.path.exists(args.download_dir):
        os.mkdir(args.download_dir)

    try:
        crawler = YCrawler(limit=args.page_limit,
                           repeat_interval=args.repeat_interval,
                           download_dir=args.download_dir,
                           dry_run=args.dry_run)
        asyncio.run(crawler.run())
    except KeyboardInterrupt:
        msg = 'Script has been stopped'
        logging.info(msg)
        sys.exit(msg)
    except Exception as e:
        msg = 'Something went wrong %s' % str(e)
        logging.error(msg)
        sys.exit(msg)
