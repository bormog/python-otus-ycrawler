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

DRY_RUN = False

REQUEST_TIMEOUT = 30


async def fetch_page(session, link, params=None):
    params = params or {}
    logging.debug('Fetch page %s with params %s' % (link, str(params)))
    try:
        async with session.get(link, params=params,
                               timeout=REQUEST_TIMEOUT,
                               allow_redirects=True,
                               max_redirects=10) as response:
            # todo fix UnicodeDecodeError if pdf file is fetched
            # example: https://www.cs.tufts.edu/~nr/cs257/archive/alfred-spector/spector85cirrus.pdf
            try:
                return await response.text()
            except Exception as e:
                logging.exception('Unknown exception while fetching link %s' % link)
    except asyncio.TimeoutError as e:
        logging.exception('Timeout while fetching link %s' % link)


async def save_page(filepath, content):
    if DRY_RUN:
        return
    else:
        logging.debug('Save file %s' % filepath)
        async with aiofiles.open(filepath, mode='w') as fw:
            await fw.write(content)
            await fw.close()


async def download_comment_link(session, uid, link):
    comment_link_content = await fetch_page(session, link)

    save_to_dir = os.path.join(DOWNLOAD_DIR, uid, 'links')
    if not os.path.exists(save_to_dir):
        os.mkdir(save_to_dir)

    t = str(int(1000 * time.time()))
    filepath = os.path.join(save_to_dir, 'comment-%s.html' % t)
    await save_page(filepath, comment_link_content)

    return uid, link


async def download_comments_links(session, uid, links):
    tasks = []
    for link in links:
        task = asyncio.create_task(download_comment_link(session, uid, link))
        tasks.append(task)
    results = await asyncio.gather(*tasks, return_exceptions=True)
    logging.debug('comments results %s' % str(results))


async def download_page(session, uid, link, dst_dir):
    logging.info('[uid = %s] Download page %s' % (uid, link))
    page_content = await fetch_page(session, link)
    if not page_content:
        return uid, None

    logging.info('[uid = %s] Save page %s' % (uid, link))
    save_to_dir = os.path.join(dst_dir, uid)
    if not os.path.exists(save_to_dir):
        os.mkdir(save_to_dir)
    filepath = os.path.join(save_to_dir, '%s.html' % uid)
    await save_page(filepath, page_content)

    logging.info('[uid = %s] Download page comments' % uid)
    comments_page = await fetch_page(session, COMMENT_PAGE_URL, params={'id': uid})
    comments_links = parse_comments(comments_page)

    # logging.info('[uid = %s] Save comments links on disk' % uid)
    # await download_comments_links(session, uid, comments_links)

    return uid, filepath


async def main():
    # todo ? do we need download new comments in old pages ?
    # todo wtf Request.raise_for_status ?

    # todo ? do we need use one session instead many?
    # todo argparse or optparse. What diff ?
    # todo make file extension by content-type

    # todo wrap response in Response class OR return original response
    # todo ? do we need use class based code instead functional ?
    logging.info('Run script')
    limit = 1
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
            for uid, _ in results:
                visited.add(uid)

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
