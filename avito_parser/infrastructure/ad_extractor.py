from __future__ import annotations

import html
import json
import re
from typing import Optional, Tuple, List, Any

from bs4 import BeautifulSoup
from loguru import logger
from pydantic import ValidationError

from avito_parser.core.models import ItemsResponse, Item


class AdExtractor:
    """
    Отвечает за:
    - поиск JSON на странице Avito,
    - преобразование JSON в список Item,
    - обогащение объявлений (sellerId),
    - извлечение просмотров из HTML страницы объявления.
    """

    def extract_ads_from_html(self, html_code: str) -> list[Item]:
        """
        Парсит HTML страницы выдачи:
        1) ищет JSON в <script>
        2) валидирует через ItemsResponse
        3) чистит пустые объявления
        4) добавляет sellerId
        """
        data_from_page = self._find_json_on_page(html_code=html_code)

        if not data_from_page:
            logger.warning("Не удалось найти JSON с объявлениями на странице")
            return []

        try:
            catalog = data_from_page.get("data", {}).get("catalog") or {}
            ads_models = ItemsResponse(**catalog)
        except ValidationError as err:
            logger.error(f"При валидации объявлений произошла ошибка: {err}")
            return []

        ads = self._clean_null_ads(ads_models.items)
        ads = self._add_seller_to_ads(ads)
        return ads

    def extract_views_from_html(self, html: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Парсит HTML страницы объявления и возвращает:
        (просмотры всего, просмотры сегодня)
        """
        soup = BeautifulSoup(html, "html.parser")

        def extract_digits(element):
            return int("".join(filter(str.isdigit, element.get_text()))) if element else None

        total = extract_digits(soup.select_one('[data-marker="item-view/total-views"]'))
        today = extract_digits(soup.select_one('[data-marker="item-view/today-views"]'))

        return total, today

    @staticmethod
    def _clean_null_ads(ads: list[Item]) -> list[Item]:
        """Отбрасывает объявления без id (такие иногда попадаются в выдаче)."""
        return [ad for ad in ads if ad.id]

    @staticmethod
    def _extract_seller_slug(data: Any) -> Optional[str]:
        """
        Ищет seller slug в данных объявления (как раньше _extract_seller_slug).
        Использует регулярку по /brands/<slug>.
        """
        match = re.search(r"/brands/([^/?#]+)", str(data))
        if match:
            return match.group(1)
        return None

    def _add_seller_to_ads(self, ads: list[Item]) -> list[Item]:
        """
        Дописывает sellerId в объявления, если удаётся вытащить slug продавца.
        """
        for ad in ads:
            seller_id = self._extract_seller_slug(ad)
            if seller_id:
                ad.sellerId = seller_id
        return ads

    @staticmethod
    def _find_json_on_page(html_code: str, data_type: str = "mime") -> dict:
        """
        Ищет нужный JSON в <script>-тегах на странице.

        Логика по сути повторяет твой find_json_on_page:
        - находим <script type="mime/invalid"> (Avito так оборачивает state)
        - декодируем html-entities
        - json.loads(...)
        - если есть 'state' -> возвращаем её
        - если есть 'data' -> возвращаем её
        """
        soup = BeautifulSoup(html_code, "html.parser")
        try:
            for _script in soup.select("script"):
                script_type = _script.get("type")

                if data_type == "mime" and script_type == "mime/invalid":
                    script_content = html.unescape(_script.text)
                    parsed_data = json.loads(script_content)

                    if "state" in parsed_data:
                        return parsed_data["state"]
                    if "data" in parsed_data:
                        logger.info("Использую parsed_data['data'] из JSON")
                        return parsed_data["data"]

            logger.error("Не найден подходящий JSON-блок на странице")
        except Exception as err:
            logger.error(f"Ошибка при разборе JSON на странице: {err}")

        return {}
