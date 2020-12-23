import argparse
import asyncio
import logging
import os
import sys
import uuid
from collections import namedtuple
from mimetypes import guess_extension

import aiofiles
import aiohttp

from parsers import parse_top_news, parse_comments

MAIN_PAGE_URL = 'https://news.ycombinator.com'
COMMENT_PAGE_URL = 'https://news.ycombinator.com/item'
ALLOWED_FILE_CONTENT_TYPES = ['application/pdf', 'image/png']
REQUEST_TIMEOUT = 30

PAGE_LIMIT = 30
REPEAT_INTERVAL = 15
DRY_RUN = False
DOWNLOAD_DIR = 'pages'

FetchResult = namedtuple('FetchResult', ['status', 'content', 'encoding', 'ext', 'link', 'params'])
DownloadResult = namedtuple('DownloadResult', ['uid', 'filepath', 'success'])


async def fetch_page(session, link, params=None):
    params = params or {}
    logging.debug('Fetch page %s with params %s' % (link, str(params)))
    try:
        async with session.get(link, params=params,
                               timeout=REQUEST_TIMEOUT,
                               allow_redirects=True,
                               max_redirects=10,
                               raise_for_status=True,
                               ssl=False) as response:

            content_type = response.headers['Content-Type']
            ext = guess_extension(content_type.partition(';')[0].strip())

            try:
                if content_type in ALLOWED_FILE_CONTENT_TYPES:
                    content = await response.read()
                else:
                    content = await response.text()

                result = FetchResult(link=link, params=params, status=response.status,
                                     encoding=response.get_encoding(), ext=ext, content=content)
                return result
            except UnicodeDecodeError:
                logging.exception('Failed fetch link %s, headers = ', (link, response.headers))
            except Exception:
                logging.exception('Unknown exception while fetching link %s' % link)

    except asyncio.TimeoutError:
        logging.exception('Timeout while fetching link %s' % link)
    except aiohttp.InvalidURL:
        logging.exception('Invalid url %s with params %s' % (link, params))
    except aiohttp.ClientResponseError as e:
        logging.exception('ClientResponseError with status %s' % str(e))
    except aiohttp.ClientOSError as e:
        logging.exception('ClientOSError %s' % str(e))

    return FetchResult(link=link, params=params, status=None, encoding=None, ext=None, content=None)


async def download_page(filepath, content):
    mode = 'wb' if isinstance(content, bytes) else 'w'
    async with aiofiles.open(filepath, mode=mode) as fw:
        await fw.write(content)
        await fw.close()


async def fetch_and_download(session, uid, link, save_to_dir, params=None, dry_run=False):
    fetch_result = await fetch_page(session, link, params)
    if not fetch_result.content:
        return DownloadResult(uid=uid, filepath=None, success=False)
    else:
        filepath = os.path.join(save_to_dir, '%s%s' % (uid, fetch_result.ext))
        if not dry_run:
            if not os.path.exists(save_to_dir):
                os.makedirs(save_to_dir, exist_ok=True)
            await download_page(filepath, fetch_result.content)
        return DownloadResult(uid=uid, filepath=filepath, success=True)


async def process_page(session, uid, link, save_to_dir, dry_run=False):
    logging.debug('[uid = %s] Download page %s' % (uid, link))
    download_result = await fetch_and_download(session, uid, link, save_to_dir, dry_run=dry_run)
    logging.debug('[uid = %s] Download result: %s' % (uid, download_result.success))

    logging.debug('[uid = %s] Download page comments' % uid)
    fetch_results = await fetch_page(session, COMMENT_PAGE_URL, params={'id': uid})
    if fetch_results.content:
        comments_links = parse_comments(fetch_results.content)
        if comments_links:
            link_tasks = []
            for link in comments_links:
                link_save_to_dir = os.path.join(save_to_dir, 'links')
                link_uid = uuid.uuid4().hex
                task = asyncio.create_task(fetch_and_download(
                    session,
                    link_uid,
                    link,
                    save_to_dir=link_save_to_dir,
                    dry_run=dry_run
                ))
                link_tasks.append(task)
                await asyncio.sleep(0)

            await asyncio.gather(*link_tasks, return_exceptions=False)
    return download_result


async def main(page_limit, repeat_interval, download_dir, dry_run):
    logging.info('Run script')
    visited = set()
    async with aiohttp.ClientSession() as session:
        while True:
            logging.info('Start fetch main page')
            fetch_result = await fetch_page(session, MAIN_PAGE_URL)
            if not fetch_result.content:
                logging.error('Main content is empty. Continue')
                continue

            top_news = parse_top_news(fetch_result.content, limit=page_limit)
            tasks = []
            for uid, link in top_news:
                if uid in visited:
                    continue
                save_to_dir = os.path.join(download_dir, uid)
                task = asyncio.create_task(process_page(session, uid, link, save_to_dir=save_to_dir, dry_run=dry_run))
                tasks.append(task)
                await asyncio.sleep(0)
            logging.info('Found %s new pages' % len(tasks))

            results = await asyncio.gather(*tasks, return_exceptions=False)
            for result in results:
                if isinstance(result, Exception):
                    logging.exception('Some unhandled exception: %s' % str(result))
                elif isinstance(result, DownloadResult):
                    visited.add(result.uid)

            logging.info('Total visited pages %s' % len(visited))

            await asyncio.sleep(repeat_interval)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description='Crawler for https://news.ycombinator.com')
    arg_parser.add_argument('--repeat_interval', default=REPEAT_INTERVAL, type=int)
    arg_parser.add_argument('--page_limit', default=PAGE_LIMIT, type=int)
    arg_parser.add_argument('--download_dir', default=DOWNLOAD_DIR, type=str)
    arg_parser.add_argument('--dry_run', default=True, type=bool)
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
        asyncio.run(main(page_limit=args.page_limit,
                         repeat_interval=args.repeat_interval,
                         download_dir=args.download_dir,
                         dry_run=args.dry_run))
    except KeyboardInterrupt:
        msg = 'Script has been stopped'
        logging.info(msg)
        sys.exit(msg)
    except Exception as e:
        msg = 'Something went wrong %s' % str(e)
        logging.info(msg)
        sys.exit(msg)
