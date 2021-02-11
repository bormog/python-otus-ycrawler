import asyncio
import functools
import logging
import os
import uuid

import aiohttp

from fetcher import URLFetcher, DownloadResult
from parsers import parse_top_news, parse_comments


MAIN_PAGE_URL = 'https://news.ycombinator.com'
COMMENT_PAGE_URL = 'https://news.ycombinator.com/item'
CONN_LIMIT_PER_HOST = 60


class YCrawler:

    def __init__(self, limit, repeat_interval, download_dir, dry_run):
        self.limit = limit
        self.repeat_interval = repeat_interval
        self.download_dir = download_dir
        self.dry_run = dry_run

        self.scheduled = set()
        self.visited = set()

    async def process_page(self, fetcher, session, uid, link, save_to_dir):
        logging.debug('[uid = %s] Download page %s' % (uid, link))
        download_result = await fetcher.fetch_and_download(session, uid, link, save_to_dir, dry_run=self.dry_run)
        logging.debug('[uid = %s] Download result: %s' % (uid, download_result.success))

        logging.debug('[uid = %s] Download page comments' % uid)
        fetch_results = await fetcher.fetch_page(session, COMMENT_PAGE_URL, params={'id': uid})
        if fetch_results.content:
            comments_links = parse_comments(fetch_results.content)
            if comments_links:
                link_tasks = []
                for link in comments_links:
                    link_save_to_dir = os.path.join(save_to_dir, 'links')
                    link_uid = uuid.uuid4().hex
                    task = asyncio.create_task(fetcher.fetch_and_download(
                        session,
                        link_uid,
                        link,
                        save_to_dir=link_save_to_dir,
                        dry_run=self.dry_run
                    ))
                    link_tasks.append(task)
                    await asyncio.sleep(0)

                await asyncio.gather(*link_tasks, return_exceptions=False)
        return download_result

    async def process_main_page(self, session, loop_number):
        fetcher = URLFetcher()
        fetch_result = await fetcher.fetch_page(session, MAIN_PAGE_URL)
        if not fetch_result.content:
            logging.error('[Loop = %d]. Main content is empty' % loop_number)
            return

        top_news = parse_top_news(fetch_result.content, limit=self.limit)
        tasks = []
        for uid, link in top_news:
            if uid in self.scheduled or uid in self.visited:
                continue
            save_to_dir = os.path.join(self.download_dir, uid)
            task = asyncio.create_task(self.process_page(fetcher, session, uid, link, save_to_dir=save_to_dir))
            self.scheduled.add(uid)
            tasks.append(task)
        logging.info('[Loop = %d]. Scheduled %s new tasks' % (loop_number, len(tasks)))

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=False)
        logging.info("[Loop = %d]. Fetcher Results: fetched %d, downloads %d, errors %s" %
                     (loop_number, fetcher.fetched, fetcher.download, fetcher.errors))
        return results

    def after_main_page_processed(self, task, loop_number):
        results = task.result()
        if not results:
            return
        for result in results:
            if isinstance(result, Exception):
                logging.exception('Some unhandled exception: %s' % str(result))
            elif isinstance(result, DownloadResult):
                self.visited.add(result.uid)
                self.scheduled.remove(result.uid)
        logging.info('[Loop = %d]. Scheduled %d pages, visited %d pages' %
                     (loop_number, len(self.scheduled), len(self.visited)))

    async def run(self):
        loop_number = 0
        connector = aiohttp.TCPConnector(limit_per_host=CONN_LIMIT_PER_HOST)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                logging.info("[Loop = %d]. Iteration start" % loop_number)

                task = asyncio.create_task(self.process_main_page(session, loop_number))
                task.add_done_callback(functools.partial(self.after_main_page_processed, loop_number=loop_number))

                logging.info("[Loop = %d]. Iteration end. Sleep now on %d sec" % (loop_number, self.repeat_interval))

                loop_number += 1
                await asyncio.sleep(self.repeat_interval)
