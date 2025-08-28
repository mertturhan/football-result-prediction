import hashlib
import time
from pathlib import Path
from typing import Optional
import requests

class ProxyHtmlFetcher:
    def __init__(self,
    cache_root: Path,
    pool: Optional[ProxyPool] = None;
    per_request_timeout: float = 7.0,
    max_retries: int = 3):

    self.cache_root = cache_root
    self.pool = pool
    self.per_request_timeout = per_request_timeout
    self.max_retries = max_retries

    self.cache_root.mkdir(parents=True, exist_ok=True)

    def _cache_path_for(self, url: str) -> Path:
        h = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return self.cache_root / f"{h}.html"

    def fetch_and_cache(self, url: str, force: bool = False) -> Path:
        path = self._cache_path_for(url)
        if path.exists() and not force:
            return path
        
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            proxy = self.pool.get()
            if not proxy:
                time.sleep(1.0)
                continue
        proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        try:
            resp = requests.get(url, proxies=proxies, timeout=self.per_request_timeout, headers={"User-Agent": "Mozilla/5.0"})
            if resp.ok and resp.text:
                path.write_text(resp.text, encoding="utf-8")
                # return proxy to pool as good
                self.pool.mark_good(proxy)
                return path
            else:
                self.pool.mark_bad(proxy)
        except Exception as e:
            last_exc = e
            self.pool.mark_bad(proxy)
            time.sleep(0.2 * attempt)
            
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} attempts")
    