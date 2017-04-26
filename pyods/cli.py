#!/usr/bin/env python3.5

import os
import argparse
import asyncio
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote, urlsplit, urlunsplit
from collections import namedtuple
from requests import Session
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing


ScrapeConfig = namedtuple("ScrapeConfig", "output loop executor base_url visited semaphore")
"""
    output: base dest dir to put downloaded files
    loop: asyncio loop object
    executor: large file download threadpool executor
    base_url: remote open dir base url to scan
    visited: list of urls already visiting/visited
"""

http = Session()


def clean_url(url):
    """
    Run a url through urlsplit and urljoin, ensuring uniform url format
    """
    return urlunsplit(urlsplit(url))


def stream_url(url):
    """
    Return a request's Response object for the given URL
    """
    return http.get(url, stream=True)


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


def stream_to_file(response, url, semaphore):
    print("STREAMING {}".format(url))
    # Stream to disk
    try:
        with open("/dev/null", "wb") as f:
            for chunk in response.iter_content(chunk_size=256 * 1024):
                f.write(chunk)
    finally:
        semaphore.release()
        response.close()


async def scrape_url(url, options):
    """
    - Request the URL
    - If HTML, parse and recurse
    - If other, download
    """

    options.visited.append(url)

    g = await options.loop.run_in_executor(None, stream_url, url)

    if g.status_code != 200:
        logging.warning("Fetch failed, code was %s", g.status_code)
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
        options.semaphore.acquire()
        options.executor.submit(stream_to_file, g, url, options.semaphore)


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)-15s %(levelname)-8s %(filename)s:%(lineno)d %(message)s")

    parser = argparse.ArgumentParser(description="Open directory scraper")
    parser.add_argument('-u', '--url', help="url to scrape")
    parser.add_argument('-o', '--output-dir', help="destination for downloaded files")
    parser.add_argument('-p', '--parallel', type=int, default=5, help="number of downloads to execute in parallel")
    args = parser.parse_args()

    logging.info("cli args: %s", args)

    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        with closing(asyncio.get_event_loop()) as loop:
            loop.set_debug(True)
            loop.set_default_executor(executor)

            base_url = clean_url(args.url)

            config = ScrapeConfig(os.path.normpath(args.output_dir),
                                  loop,
                                  executor,
                                  base_url,
                                  [],
                                  asyncio.Semaphore(value=args.parallel))

            downloader = asyncio.ensure_future(scrape_url(base_url, config), loop=loop)
            try:
                loop.set_debug(True)
                loop.run_until_complete(downloader)
            finally:
                logging.debug("Escaped main loop")


if __name__ == '__main__':
    main()
