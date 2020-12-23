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
PAGE_LIMIT = 30
DOWNLOAD_DIR = 'pages'
REPEAT_INTERVAL = 5

DRY_RUN = False

REQUEST_TIMEOUT = 15

FetchResult = namedtuple('FetchResult',
                         ['status', 'content', 'encoding', 'ext', 'link', 'params'])
DownloadResult = namedtuple('DownloadResult', ['uid', 'filepath', 'success'])


# class Page:
#
#     def __init__(self, uid, parent_uid=None, status=None, ext=None, content=None, link=None, params=None,
#                  save_to=None, filepath=None):
#         self.uid = uid
#         self.parent_uid = None
#         self.status = status
#         self.ext = ext
#         self.content = content
#         self.link = link
#         self.save_to = None
#         self.filepath = None
#         self.attempt = 0
#
#     async def fetch(self, session):
#         pass
#
#     async def save(self):
#         pass


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
                if content_type == 'application/pdf':
                    content = await response.read()
                else:
                    content = await response.text()

                result = FetchResult(link=link, params=params, status=response.status,
                                     encoding=response.get_encoding(), ext=ext, content=content)
                return result
            except UnicodeDecodeError:
                logging.exception(response.headers)
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
    if not DRY_RUN:
        mode = 'wb' if isinstance(content, bytes) else 'w'
        async with aiofiles.open(filepath, mode=mode) as fw:
            await fw.write(content)
            await fw.close()


async def fetch_and_download(session, uid, link, save_to_dir, params=None):
    fetch_result = await fetch_page(session, link, params)
    if not fetch_result.content:
        return DownloadResult(uid=uid, filepath=None, success=False)
    else:
        filepath = os.path.join(save_to_dir, '%s%s' % (uid, fetch_result.ext))
        if not DRY_RUN:
            if not os.path.exists(save_to_dir):
                os.makedirs(save_to_dir, exist_ok=True)
            await download_page(filepath, fetch_result.content)
        return DownloadResult(uid=uid, filepath=filepath, success=True)


async def process_page(session, uid, link, save_to_dir):
    logging.debug('[uid = %s] Download page %s' % (uid, link))
    download_result = await fetch_and_download(session, uid, link, save_to_dir)
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
                    save_to_dir=link_save_to_dir
                ))
                link_tasks.append(task)

            await asyncio.gather(*link_tasks, return_exceptions=False)
    return download_result


async def main():
    # todo argparse or optparse.
    # todo ? do we need use class based code instead functional ?
    logging.info('Run script')
    limit = 3
    visited = set()
    async with aiohttp.ClientSession() as session:
        while True:
            logging.info('Start fetch main page')
            fetch_result = await fetch_page(session, MAIN_PAGE_URL)
            if not fetch_result.content:
                logging.error('Main content is empty. Continue')
                continue

            top_news = parse_top_news(fetch_result.content, limit=limit)
            tasks = []
            for uid, link in top_news:
                if uid in visited:
                    continue
                save_to_dir = os.path.join(DOWNLOAD_DIR, uid)
                task = asyncio.create_task(process_page(session, uid, link, save_to_dir=save_to_dir))
                tasks.append(task)
            logging.info('Found %s new pages' % len(tasks))

            results = await asyncio.gather(*tasks, return_exceptions=False)
            for result in results:
                if isinstance(result, Exception):
                    logging.exception('Some unhandled exception: %s' % str(result))
                elif isinstance(result, DownloadResult):
                    visited.add(result.uid)

            logging.info('Total visited pages %s' % len(visited))

            await asyncio.sleep(REPEAT_INTERVAL)

            if limit < PAGE_LIMIT:
                limit += 1


if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
        filename=None,
        level=logging.INFO
    )
    if not os.path.exists(DOWNLOAD_DIR):
        os.mkdir(DOWNLOAD_DIR)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        msg = 'Script has been stopped'
        logging.info(msg)
        sys.exit(msg)
    except Exception:
        msg = 'Something went wrong'
        logging.info(msg)
        sys.exit(msg)
