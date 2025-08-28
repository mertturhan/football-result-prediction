import threading
import time
import random
import requests
from queue import Queue, Empty
from typing import Optional, Dict, List

DEFAULT_SOURCES = [
    "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=7000&country=all&ssl=all&anonymity=all",
    "https://www.proxyscan.io/download?type=http",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
]

VALIDATION_URLS = [
    "https://api.ipify.org",
    "https://checkip.amazonaws.com",
    "https://ifconfig.me/ip",
    "https://icanhazip.com",
    "https://ip.seeip.org",
    "https://www.google.com/generate_204",
]

def _parse_candidates(text: str) -> List[str]:
    candidates = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # accept IP:PORT tokens anywhere in the line
        parts = [p for p in line.replace(",", ":").split(":") if ":" in p]
        for p in parts:
            host, _, port = p.partition(":")
            if host and port.isdigit():
                candidates.append(f"{host.strip()}:{port.strip()}")
    return candidates

class ProxyPool:
    def __init__(self, 
    min_pool_size: int = 100, 
    max_pool_size: int = 500, 
    validate_urls: None,
    target_probe_url: str | None = "https://fbref.com/robots.txt"):

    self.min_pool_size = min_pool_size
    self.max_pool_size = max_pool_size
    self.validate_urls = validate_urls or VALIDATION_URLS
    self.target_probe_url = target_probe_url

    self._lock = threading.Lock()
    self._seen = set()
    self._pool = Queue()
    self._last_refill = 0.0

    def _fetch_from_sources(self) -> List[str]:
        candidates: List[str] = []
        for source in DEFAULT_SOURCES:
            try:
                resp = requests.get(url, timeout=10)
                if resp.ok and resp.text:
                    candidates.extend(_parse_candidates(resp.text))
            except Exception:
                continue
        random.shuffle(candidates)
        return candidates[:self.max_pool_size]

    def _validate_proxy(self, proxy: str, timeout: float = 7.0) -> bool:
        proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        #generic connectivity/IP check (any one success is fine)
        for url in self.validation_urls:
            try:
                resp = s.get(url, proxies=proxies, timeout=timeout, allow_redirects=False)
                if resp.status_code in (200, 204) and (resp.text is not None):
                    break
            except Exception:
                continue
        else:
            return False
        
        if self.target_probe_url:
            try:
                r2 = s.head(self.target_probe_url, proxies=proxies, timeout=timeout, allow_redirects=True)
                if r2.status_code >= 400:
                    return False
            except Exception:
                return False
        return True

    def _refill_if_needed(self) -> None:
        with self._lock:
            should_refill = (self._pool.qsize() < self.min_pool_size) and (time.time() - self._last_refill > 2.0)
            if not should_refill:
                return
            self._last_refill = time.time()
        candidates = self._fetch_from_sources()
        for proxy in candidates:
            with self._lock:
                if proxy in self._seen:
                    continue
                self._seen.add(proxy)
            if self._validate_proxy(proxy):
                self._pool.put(proxy)

    def get(self, wait: float = 5.0) -> Optional[str]:
        self._refill_if_needed()
        try:
            return self._pool.get(timeout=wait)
        except Empty:
            # last-resort refill and try again quickly
            self._refill_if_needed()
            try:
                return self._pool.get(timeout=wait)
            except Empty:
                return None
            
    def mark_bad(self, proxy: str) -> None:
            # Do not reinsert; it's implicitly dropped
            pass

        def mark_good(self, proxy: str) -> None:
            # Optionally reinsert good proxies to keep pool healthy
            if proxy:
                self._pool.put(proxy)
            