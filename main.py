import asyncio
import logging
import os
import sys
import time

import aiofiles
import aiohttp

from parsers import parse_top_news, parse_comments

MAIN_PAGE_URL = 'https://news.ycombinator.com'
COMMENT_PAGE_URL = 'https://news.ycombinator.com/item'
PAGE_LIMIT = 30
DOWNLOAD_DIR = 'pages'
REPEAT_INTERVAL = 5

DRY_RUN = True

REQUEST_TIMEOUT = 30


async def fetch_page(session, link, params=None):
    params = params or {}
    logging.debug('Fetch page %s with params %s' % (link, str(params)))
    try:
        async with session.get(link, params=params,
                               timeout=REQUEST_TIMEOUT,
                               allow_redirects=True,
                               max_redirects=10,
                               raise_for_status=True) as response:
            # todo fix UnicodeDecodeError if pdf file is fetched
            # example: https://www.cs.tufts.edu/~nr/cs257/archive/alfred-spector/spector85cirrus.pdf
            try:
                return await response.text()
            except Exception as e:
                logging.exception('Unknown exception while fetching link %s' % link)
    except asyncio.TimeoutError:
        logging.exception('Timeout while fetching link %s' % link)
    except aiohttp.ClientResponseError as e:
        logging.error('ClientResponseError with status %s' % str(e))


async def save_page(filepath, content):
    if not DRY_RUN:
        async with aiofiles.open(filepath, mode='w') as fw:
            await fw.write(content)
            await fw.close()


async def download_comment_link(session, link, dst_dir):
    comment_link_content = await fetch_page(session, link)
    if not comment_link_content:
        return link, None

    filename = str(int(1000 * time.time()))
    filepath = os.path.join(dst_dir, '%s.html' % filename)
    await save_page(filepath, comment_link_content)
    return link, filepath


async def download_page(session, uid, link, dst_dir):
    logging.info('[uid = %s] Download page %s' % (uid, link))
    page_content = await fetch_page(session, link)
    if not page_content:
        return uid, None

    logging.debug('[uid = %s] Save page %s' % (uid, link))
    save_to_dir = os.path.join(dst_dir, uid)
    if not os.path.exists(save_to_dir):
        os.mkdir(save_to_dir)
    filepath = os.path.join(save_to_dir, '%s.html' % uid)
    await save_page(filepath, page_content)

    logging.debug('[uid = %s] Download page comments' % uid)
    comments_page = await fetch_page(session, COMMENT_PAGE_URL, params={'id': uid})
    comments_links = parse_comments(comments_page)

    if comments_links:
        links_dir = os.path.join(save_to_dir, 'links')
        if not os.path.exists(links_dir):
            os.mkdir(links_dir)

        tasks = []
        for link in comments_links:
            task = asyncio.create_task(download_comment_link(session, link, dst_dir=links_dir))
            tasks.append(task)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logging.debug('comments results %s' % str(results))

    return uid, filepath


async def main():
    # todo ? do we need download new comments in old pages ?
    # todo wtf Request.raise_for_status ?

    # todo ? do we need use one session instead many?
    # todo argparse or optparse.
    # todo make file extension by content-type

    # todo wrap response in Response class OR return original response
    # todo ? do we need use class based code instead functional ?
    logging.info('Run script')
    limit = 5
    visited = set()
    async with aiohttp.ClientSession() as session:
        while True:
            logging.info('Start fetch main page')
            main_page_content = await fetch_page(session, MAIN_PAGE_URL)
            if not main_page_content:
                logging.error('Main content is empty. Continue')
                continue

            top_news = parse_top_news(main_page_content, limit=limit)

            tasks = []
            for uid, link in top_news:
                if uid in visited:
                    continue
                task = asyncio.create_task(download_page(session, uid, link, dst_dir=DOWNLOAD_DIR))
                tasks.append(task)
            logging.info('Found %s new pages' % len(tasks))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            # todo check if not error in results
            for result in results:
                if isinstance(result, Exception):
                    logging.exception('Some exception: %s' % str(result))
                else:
                    visited.add(uid)
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
