import asyncio
import json
import time
from typing import Optional

from curl_cffi import requests
from loguru import logger
from requests.cookies import RequestsCookieJar

from avito_parser.core.dto import AvitoConfig, Proxy
from avito_parser.infrastructure.common_data import HEADERS
from avito_parser.infrastructure.get_cookies import get_cookies


class AvitoHttpClient:
    """
    Инкапсулирует:
    - curl_cffi Session
    - заголовки (HEADERS + user-agent от Playwright)
    - cookies (файл cookies.json + обновление через Playwright)
    - прокси и смену IP
    - логику ретраев/429/403/302
    """

    def __init__(
        self,
        config: AvitoConfig,
        proxy: Optional[Proxy] = None,
        stop_event=None,
    ):
        self.config = config
        self.proxy = proxy
        self.stop_event = stop_event

        self.session = requests.Session()
        self.headers = HEADERS.copy()
        self.cookies: Optional[dict] = None

        self.good_request_count = 0
        self.bad_request_count = 0

        self.load_cookies()

    def load_cookies(self) -> None:
        """Загружает cookies из JSON-файла в requests.Session."""
        try:
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            jar = RequestsCookieJar()
            for k, v in cookies.items():
                jar.set(k, v)
            self.session.cookies.update(jar)
        except FileNotFoundError:
            pass

    def save_cookies(self) -> None:
        """Сохраняет cookies из requests.Session в JSON-файл."""
        with open("cookies.json", "w") as f:
            json.dump(self.session.cookies.get_dict(), f)

    def _refresh_cookies(self, max_retries: int = 1, delay: float = 2.0) -> bool:
        """
        Обновляет cookies через Playwright (get_cookies.py).
        Заодно обновляет user-agent в заголовках.
        """
        for attempt in range(1, max_retries + 1):
            if self.stop_event and self.stop_event.is_set():
                return False

            try:
                cookies, user_agent = asyncio.run(
                    get_cookies(proxy=self.proxy, headless=True, stop_event=self.stop_event)
                )
                if cookies:
                    logger.info(f"[http_client.get_cookies] Успешно получены cookies с попытки {attempt}")
                    self.headers["user-agent"] = user_agent
                    self.cookies = cookies
                    return True
                else:
                    raise ValueError("Пустой результат cookies")
            except Exception as e:
                logger.warning(f"[http_client.get_cookies] Попытка {attempt} не удалась: {e}")
                if attempt < max_retries:
                    time.sleep(delay * attempt) 
                else:
                    logger.error(
                        f"[http_client.get_cookies] Все {max_retries} попытки не удались"
                    )
                    return False

    def _change_ip(self) -> bool:
        """Смена IP через config.proxy_change_url (как было в AvitoParse.change_ip)."""
        if not self.config.proxy_change_url:
            logger.info("Сейчас бы была смена ip, но мы без прокси")
            return False
        logger.info("Меняю IP")
        try:
            res = requests.get(url=self.config.proxy_change_url, verify=False)
            if res.status_code == 200:
                logger.info("IP изменен")
                return True
        except Exception as err:
            logger.info(f"При смене ip возникла ошибка: {err}")
        return False

    def fetch(self, url: str, retries: int = 3, backoff_factor: int = 1) -> Optional[str]:
        proxy_data = None
        if self.proxy:
            proxy_data = {
                "https": f"http://{self.config.proxy_string}"
            }

        for attempt in range(1, retries + 1):
            if self.stop_event and self.stop_event.is_set():
                return None

            try:
                response = self.session.get(
                    url=url,
                    headers=self.headers,
                    proxies=proxy_data,
                    cookies=self.cookies,
                    impersonate="chrome",
                    timeout=20,
                    verify=False,
                )
                logger.debug(f"Попытка {attempt}: {response.status_code}")

                if response.status_code >= 500:
                    raise requests.RequestsError(f"Ошибка сервера: {response.status_code}")

                if response.status_code == 429:
                    self.bad_request_count += 1
                    self.session = requests.Session()
                    self._change_ip()
                    if attempt >= 3:
                        self._refresh_cookies()
                    raise requests.RequestsError(f"Слишком много запросов: {response.status_code}")

                if response.status_code in [403, 302]:
                    self._refresh_cookies()
                    raise requests.RequestsError(f"Заблокирован: {response.status_code}")

                self.save_cookies()
                self.good_request_count += 1
                return response.text

            except requests.RequestsError as e:
                logger.debug(f"Попытка {attempt} закончилась неуспешно: {e}")
                if attempt < retries:
                    sleep_time = backoff_factor * attempt
                    logger.debug(f"Повтор через {sleep_time} секунд...")
                    time.sleep(sleep_time)
                else:
                    logger.info("Все попытки были неуспешными")
                    return None
