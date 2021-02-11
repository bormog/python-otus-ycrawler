import os
import asyncio
import aiohttp
import aiofiles
import logging
from collections import namedtuple
from mimetypes import guess_extension

ALLOWED_FILE_CONTENT_TYPES = ['application/pdf', 'image/png']
REQUEST_TIMEOUT = 30

FetchResult = namedtuple('FetchResult', ['status', 'content', 'encoding', 'ext', 'link', 'params'])
DownloadResult = namedtuple('DownloadResult', ['uid', 'filepath', 'success'])


class URLFetcher:

    def __init__(self):
        self.fetched = 0
        self.download = 0
        self.errors = 0

    async def fetch_page(self, session, link, params=None):
        params = params or {}
        logging.debug('Fetch page %s with params %s' % (link, str(params)))
        self.fetched += 1
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
                    self.errors += 1
                    logging.debug('Failed fetch link %s, headers = ', (link, response.headers))
                except Exception:
                    self.errors += 1
                    logging.debug('Unknown exception while fetching link %s' % link)

        except asyncio.TimeoutError:
            self.errors += 1
            logging.debug('Timeout while fetching link %s and params %s' % (link, params))
        except aiohttp.InvalidURL:
            self.errors += 1
            logging.debug('Invalid url %s with params %s' % (link, params))
        except aiohttp.ClientResponseError as e:
            self.errors += 1
            logging.debug('ClientResponseError with status %s, message %s, link %s and params %s' % (
            e.status, e.message, link, params))
        except aiohttp.ClientOSError:
            self.errors += 1
            logging.debug('ClientOSError link %s and params %s' % (link, params))

        return FetchResult(link=link, params=params, status=None, encoding=None, ext=None, content=None)

    async def download_page(self, filepath, content):
        mode = 'wb' if isinstance(content, bytes) else 'w'
        async with aiofiles.open(filepath, mode=mode) as fw:
            await fw.write(content)
            await fw.close()
            self.download += 1

    async def fetch_and_download(self, session, uid, link, save_to_dir, params=None, dry_run=False):
        fetch_result = await self.fetch_page(session, link, params)
        if not fetch_result.content:
            return DownloadResult(uid=uid, filepath=None, success=False)
        else:
            filepath = os.path.join(save_to_dir, '%s%s' % (uid, fetch_result.ext))
            if not dry_run:
                if not os.path.exists(save_to_dir):
                    os.makedirs(save_to_dir, exist_ok=True)
                await self.download_page(filepath, fetch_result.content)
            return DownloadResult(uid=uid, filepath=filepath, success=True)
