#!/usr/bin/env python3.5

import os
import argparse
import asyncio
import logging
from fnmatch import fnmatch
from time import sleep
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote, urlsplit, urlunsplit
from collections import namedtuple
from requests import Session
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing


ScrapeConfig = namedtuple("ScrapeConfig", "output loop executor base_url visited futures semaphore delay exclude")
"""
    output: base dest dir to put downloaded files
    loop: asyncio loop object
    executor: large file download threadpool executor
    base_url: remote open dir base url to scan
    visited: list of urls already visiting/visited
"""


class AlreadyDownloadedException(Exception):
    pass


http = Session()


def clean_url(url):
    """
    Run a url through urlsplit and urljoin, ensuring uniform url format
    """
    return urlunsplit(urlsplit(url))


def stream_url(url, kwargs):
    """
    Return a request's Response object for the given URL
    """
    return http.get(url, stream=True, **kwargs)


def get_links(content):
    """
    Parse and return hrefs for all links on a given page
    :param content: html body to scan
    """
    doc = BeautifulSoup(content, "html.parser")
    for link in doc.find_all("a"):
        href = link.attrs.get('href', None)
        if href:
            yield href


def stream_to_file(response, url, options, local_path):
    if not local_path.startswith(options.output):
        raise Exception("Aborted: directory traversal detected!")

    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        if os.path.exists(local_path):
            response.close()
            # Local file exists, restart request with range
            fsize = os.path.getsize(local_path)
            remote_size = int(response.headers.get("Content-length"))
            if fsize == remote_size:
                raise AlreadyDownloadedException("Already downloaded")

            logging.info("{} already exists, restarting request with range {}-{}".format(local_path, fsize,
                                                                                         remote_size))
            if options.delay:
                sleep(options.delay)

            logging.warning("Downloading {} to {}".format(url, local_path))
            response = stream_url(url, {"headers": {"Range": "bytes={}-{}".format(fsize, remote_size)}})
            response.raise_for_status()  # TODO: clobber file and restart w/ no range header if range not satisfiable

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=256 * 1024):
                f.write(chunk)
    finally:
        options.semaphore.release()
        try:
            response.close()
        except:
            pass
    return (url, local_path)


async def scrape_url(url, options, skip=False):
    """
    - Request the URL
    - If HTML, parse and recurse
    - If other, download
    """
    options.visited.append(url)

    g = await options.loop.run_in_executor(None, stream_url, url, {"headers": {"Accept-encoding": "identity"}})

    if g.status_code != 200:
        logging.error("Fetch failed, code was %s", g.status_code)
        return

    content_type = g.headers.get("Content-Type", "")
    if "text/html" in content_type:
        # HTML page, parse it
        for link in get_links(g.text):
            link_dest = clean_url(urljoin(url, link))
            if not link_dest.startswith(options.base_url):
                # Link leads outside the base_url, skip it
                continue
            if link_dest not in options.visited:
                # link is valid and not seen before, visit it
                logging.info("Visiting %s", url)
                await scrape_url(link_dest, options)
        g.close()
    else:
        # Actual file, download it
        # await download_file(g, url, options)
        url_suffix = unquote(url[len(options.base_url):])
        local_path = os.path.normpath(os.path.join(options.output, url_suffix))
        for pattern in options.exclude:
            if fnmatch(url_suffix, pattern):
                logging.info("Excluded: '%s' on pattern '%s'", url_suffix, pattern)
                return
        await options.semaphore.acquire()
        options.futures.append((options.executor.submit(stream_to_file, g, url, options, local_path), url, ))
        # Purge completed futures
        for item in options.futures[:]:
            future, url = item
            if future.done():
                options.futures.remove(item)
                exc = future.exception()
                if exc:
                    if type(exc) is AlreadyDownloadedException:
                        logging.info("ALREADY COMPLETE: url: %s", url)
                    else:
                        logging.error("FAILED: %s: %s", url, exc)
                else:
                    logging.warning("COMPLETED downloading url %s to %s", *future.result())


def main():
    parser = argparse.ArgumentParser(description="Open directory scraper")
    parser.add_argument('-u', '--url', help="url to scrape")
    parser.add_argument('-o', '--output-dir', help="destination for downloaded files")
    parser.add_argument('-p', '--parallel', type=int, default=5, help="number of downloads to execute in parallel")
    parser.add_argument('-c', '--clobber', action="store_true", help="clobber existing files instead of resuming")
    parser.add_argument('-d', '--delay', type=int, default=0, help="delay between requests")
    parser.add_argument('-e', '--exclude', default=[], nargs="+", help="exclude patterns")
    parser.add_argument('-f', '--exclude-from', help="exclude patterns from file")
    parser.add_argument('-v', '--verbose', action="store_true", help="enable info logging")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format="%(asctime)-15s %(levelname)-8s %(filename)s:%(lineno)d %(message)s")

    logging.debug("cli args: %s", args)

    excludes = list(args.exclude)
    if args.exclude_from:
        with open(args.exclude_from) as f:
            excludes += [l.strip() for l in f.readlines() if l.strip()]

    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        with closing(asyncio.get_event_loop()) as loop:
            loop.set_debug(True)
            loop.set_default_executor(executor)

            base_url = clean_url(args.url)

            config = ScrapeConfig(os.path.normpath(args.output_dir),
                                  loop,
                                  executor,
                                  base_url,
                                  [],  # visited urls
                                  [],  # futures
                                  asyncio.Semaphore(value=args.parallel),
                                  args.delay,
                                  excludes)

            downloader = asyncio.ensure_future(scrape_url(base_url, config), loop=loop)
            try:
                loop.set_debug(True)
                loop.run_until_complete(downloader)
            finally:
                logging.debug("Escaped main loop")


if __name__ == '__main__':
    main()
