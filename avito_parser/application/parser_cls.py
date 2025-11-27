import asyncio
import html
import json
import random
import re
import time
from urllib.parse import unquote, urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from loguru import logger
from pydantic import ValidationError

from avito_parser.infrastructure.db_service import SQLiteDBHandler
from avito_parser.core.dto import Proxy, AvitoConfig
from avito_parser.infrastructure.http_client import AvitoHttpClient
from avito_parser.infrastructure.ad_extractor import AdExtractor
from avito_parser.infrastructure.hide_private_data import log_config
from avito_parser.application.config_loader import load_avito_config
from avito_parser.core.models import ItemsResponse, Item
from avito_parser.core.filters import FilterContext, apply_filters
from avito_parser.infrastructure.tg_sender import SendAdToTg
from version import VERSION
from avito_parser.infrastructure.xlsx_service import XLSXHandler
from avito_parser.application.use_cases import ParseLinksUseCase


from avito_parser.application.pipelines import (
    AdProcessor,
    SQLiteViewedRepository,
    TelegramNotifier,
)

DEBUG_MODE = False

logger.add("logs/app.log", rotation="5 MB", retention="5 days", level="DEBUG")


class AvitoParse:
    def __init__(self, config: AvitoConfig, stop_event=None):
        self.config = config
        self.stop_event = stop_event

        self.proxy_obj = self.get_proxy_obj()
        self.db_handler = SQLiteDBHandler()
        self.tg_handler = self.get_tg_handler()
        self.xlsx_handler = XLSXHandler(self.__get_file_title())

        self.http_client = AvitoHttpClient(
            config=self.config,
            proxy=self.proxy_obj,
            stop_event=self.stop_event,
        )

        self.ad_extractor = AdExtractor()

        self.ad_processor = AdProcessor(
            viewed_repo=SQLiteViewedRepository(self.db_handler),
            notifiers=(
                [TelegramNotifier(self.tg_handler, one_time_start=self.config.one_time_start)]
                if self.tg_handler
                else []
            ),
        )

        log_config(config=self.config, version=VERSION)

    def get_tg_handler(self) -> SendAdToTg | None:
        if all([self.config.tg_token, self.config.tg_chat_id]):
            return SendAdToTg(bot_token=self.config.tg_token, chat_id=self.config.tg_chat_id)
        return None

    def get_proxy_obj(self) -> Proxy | None:
        if all([self.config.proxy_string, self.config.proxy_change_url]):
            return Proxy(
                proxy_string=self.config.proxy_string,
                change_ip_link=self.config.proxy_change_url
            )
        logger.info("Работаем без прокси")
        return None

    def parse(self) -> None:
        """
        Фасад для запуска основного use-case обхода всех ссылок.
        Внешний код (GUI, в будущем — бот) по-прежнему вызывает AvitoParse.parse(),
        но всю работу делает ParseLinksUseCase.
        """
        use_case = ParseLinksUseCase(self)
        use_case.run()


    def filter_ads(self, ads: list[Item]) -> list[Item]:
        """
        Применяет набор фильтров к объявлениям.
        Логику самих фильтров мы вынесли в avito_parser.core.filters.
        """
        ctx = FilterContext(
            config=self.config,
            is_viewed=self.is_viewed,
        )
        return apply_filters(ads, ctx)

    def parse_views(self, ads: list[Item]) -> list[Item]:
        if not self.config.parse_views:
            return ads

        logger.info("Начинаю парсинг просмотров")

        for ad in ads:
            try:
                html_code_full_page = self.http_client.fetch(
                    url=f"https://www.avito.ru{ad.urlPath}",
                    retries=self.config.max_count_of_retry,
                )
                if not html_code_full_page:
                    continue

                ad.total_views, ad.today_views = self.ad_extractor.extract_views_from_html(
                    html_code_full_page
                )
                delay = random.uniform(0.1, 0.9)
                time.sleep(delay)
            except Exception as err:
                logger.warning(f"Ошибка при парсинге {ad.urlPath}: {err}")
                continue

        return ads

    def is_viewed(self, ad: Item) -> bool:
        """Проверяет, смотрели мы это или нет"""
        return self.db_handler.record_exists(record_id=ad.id, price=ad.priceDetailed.value)

    def __get_file_title(self) -> str:
        """Определяет название файла"""
        title_file = 'all'
        if self.config.keys_word_white_list:
            title_file = "-".join(list(map(str.lower, self.config.keys_word_white_list)))
            if len(title_file) > 50:
                title_file = title_file[:50]

        return f"result/{title_file}.xlsx"

    def _save_data(self, ads: list[Item]) -> None:
        """Сохраняет результат в файл keyword*.xlsx и в БД"""
        try:
            self.xlsx_handler.append_data_from_page(ads=ads)
        except Exception as err:
            logger.info(f"При сохранении в Excel ошибка {err}")

    def get_next_page_url(self, url: str):
        """Получает следующую страницу"""
        try:
            url_parts = urlparse(url)
            query_params = parse_qs(url_parts.query)
            current_page = int(query_params.get('p', [1])[0])
            query_params['p'] = current_page + 1
            if self.config.one_time_start:
                logger.debug(f"Страница {current_page}")

            new_query = urlencode(query_params, doseq=True)
            next_url = urlunparse((url_parts.scheme, url_parts.netloc, url_parts.path, url_parts.params, new_query,
                                   url_parts.fragment))
            return next_url
        except Exception as err:
            logger.error(f"Не смог сформировать ссылку на следующую страницу для {url}. Ошибка: {err}")

    def _should_stop(self) -> bool:
        """Проверяет, надо ли останавливаться (stop_event от GUI/бота)."""
        return bool(self.stop_event and self.stop_event.is_set())

    def _get_page_html(self, url: str) -> str | None:
        """Возвращает HTML страницы или None, если получить не удалось."""
        if DEBUG_MODE:
            return open("response.txt", "r", encoding="utf-8").read()

        html_code = self.http_client.fetch(url=url, retries=self.config.max_count_of_retry)
        if not html_code:
            logger.warning(
                f"Не удалось получить HTML для {url}, пробую заново через {self.config.pause_between_links} сек."
            )
            time.sleep(self.config.pause_between_links)
        return html_code

    def _parse_ads_from_html(self, html_code: str) -> list[Item]:
        """
        Делегирует парсинг и обогащение объявлений в AdExtractor.
        """
        return self.ad_extractor.extract_ads_from_html(html_code)

    def _process_ads_batch(self, ads: list[Item], ads_in_link: list[Item]) -> None:
        """
        Общий конвейер обработки пачки объявлений с одной страницы:
        фильтры → просмотры → БД/TG → Excel.
        """
        if not ads:
            return

        filtered_ads = self.filter_ads(ads=ads)

        filtered_ads = self.parse_views(ads=filtered_ads)

        if not filtered_ads:
            return

        if hasattr(self, "ad_processor") and self.ad_processor:
            self.ad_processor.process(filtered_ads)
        else:
            if self.tg_handler and not self.config.one_time_start:
                self._send_to_tg(ads=filtered_ads)
            self.__save_viewed(ads=filtered_ads)

        if self.config.save_xlsx:
            ads_in_link.extend(filtered_ads)

    def _process_url(self, index: int, url: str) -> list[Item]:
        """Обрабатывает одну ссылку (несколько страниц по ней)."""
        ads_in_link: list[Item] = []
        current_url = url

        for page_index in range(self.config.count):
            if self._should_stop():
                break

            if not self.xlsx_handler and self.config.one_file_for_link:
                self.xlsx_handler = XLSXHandler(f"result/{index + 1}.xlsx")

            html_code = self._get_page_html(current_url)
            if not html_code:
                continue

            ads = self._parse_ads_from_html(html_code=html_code)
            if not ads:
                logger.info("Объявления закончились, заканчиваю работу с данной ссылкой")
                break

            self._process_ads_batch(ads=ads, ads_in_link=ads_in_link)

            current_url = self.get_next_page_url(url=current_url)

            logger.info(f"Пауза {self.config.pause_between_links} сек.")
            time.sleep(self.config.pause_between_links)

        return ads_in_link
