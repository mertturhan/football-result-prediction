from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, List
import logging
from proxy_pool import ProxyPool
from proxy_html_cache import ProxyHtmlFetcher

logger = logging.getLogger(__name__)

def prefetch_urls(
    urls: Iterable[str], 
    cache_dir: Path,
    max_workers: int = 20) -> List[Path]:

    pool = ProxyPool()
    fetcher = ProxyHtmlFetcher(cache_dir, pool)
    results: List[Path] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetcher.fetch_and_cache, url) for url in urls]
        for fut in as_completed(futures):
            url = futures[fut]
            try:
                path = fut.result()
                results.append(path)
                logger.info("Cached %s -> %s", url, path)
            except Exception as e:
                logger.warning("Failed to cache %s: %s", url, e)

    return results
    