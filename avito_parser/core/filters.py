from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, List, Iterable

from loguru import logger

from avito_parser.core.dto import AvitoConfig
from avito_parser.core.models import Item


AdList = list[Item]
IsViewedFn = Callable[[Item], bool]


@dataclass
class FilterContext:
    """
    Контекст для фильтров: настройки + функция проверки, видели объявление или нет.
    """
    config: AvitoConfig
    is_viewed: IsViewedFn


def _safe_text(ad: Item) -> str:
    title = ad.title or ""
    desc = ad.description or ""
    return (title + " " + desc).lower()


def _is_phrase_in_ad(ad: Item, phrases: Iterable[str]) -> bool:
    text = _safe_text(ad)
    return any((phrase or "").lower() in text for phrase in phrases)


def _is_recent(timestamp_ms: int | None, max_age_seconds: int) -> bool:
    if not timestamp_ms:
        return False
    now = datetime.utcnow()
    published_time = datetime.utcfromtimestamp(timestamp_ms / 1000)
    return (now - published_time) <= timedelta(seconds=max_age_seconds)


def _add_promotion_flag(ads: AdList) -> AdList:
    """
    Заполняет ad.isPromotion на основе блока iva.DateInfoStep, как у тебя в _add_promotion_to_ads.
    """
    for ad in ads:
        try:
            ad.isPromotion = any(
                v.get("title") == "Продвинуто"
                for step in (ad.iva or {}).get("DateInfoStep", [])
                for v in (step.payload or {}).get("vas", [])
            )
        except Exception as err:
            logger.debug(f"Ошибка при определении isPromotion: {err}")
    return ads



def filter_viewed(ads: AdList, ctx: FilterContext) -> AdList:
    try:
        return [ad for ad in ads if not ctx.is_viewed(ad)]
    except Exception as err:
        logger.debug(f"Ошибка при проверке объявления по признаку смотрели/не смотрели: {err}")
        return ads


def filter_by_price_range(ads: AdList, ctx: FilterContext) -> AdList:
    try:
        return [
            ad for ad in ads
            if ad.priceDetailed
            and ctx.config.min_price <= ad.priceDetailed.value <= ctx.config.max_price
        ]
    except Exception as err:
        logger.debug(f"Ошибка при фильтрации по цене: {err}")
        return ads


def filter_by_black_keywords(ads: AdList, ctx: FilterContext) -> AdList:
    if not ctx.config.keys_word_black_list:
        return ads
    try:
        return [
            ad for ad in ads
            if not _is_phrase_in_ad(ad, ctx.config.keys_word_black_list)
        ]
    except Exception as err:
        logger.debug(f"Ошибка при проверке объявлений по списку стоп-слов: {err}")
        return ads


def filter_by_white_keywords(ads: AdList, ctx: FilterContext) -> AdList:
    if not ctx.config.keys_word_white_list:
        return ads
    try:
        return [
            ad for ad in ads
            if _is_phrase_in_ad(ad, ctx.config.keys_word_white_list)
        ]
    except Exception as err:
        logger.debug(f"Ошибка при проверке объявлений по списку обязательных слов: {err}")
        return ads


def filter_by_address(ads: AdList, ctx: FilterContext) -> AdList:
    if not ctx.config.geo:
        return ads
    try:
        return [
            ad for ad in ads
            if ad.geo
            and ad.geo.formattedAddress
            and ctx.config.geo in ad.geo.formattedAddress
        ]
    except Exception as err:
        logger.debug(f"Ошибка при проверке объявлений по адресу: {err}")
        return ads


def filter_by_seller(ads: AdList, ctx: FilterContext) -> AdList:
    if not ctx.config.seller_black_list:
        return ads
    try:
        return [
            ad for ad in ads
            if not ad.sellerId or ad.sellerId not in ctx.config.seller_black_list
        ]
    except Exception as err:
        logger.debug(f"Ошибка при отсеивании объявления с продавцами из черного списка: {err}")
        return ads


def filter_by_recent_time(ads: AdList, ctx: FilterContext) -> AdList:
    if not ctx.config.max_age:
        return ads
    try:
        return [
            ad for ad in ads
            if _is_recent(timestamp_ms=ad.sortTimeStamp, max_age_seconds=ctx.config.max_age)
        ]
    except Exception as err:
        logger.debug(f"Ошибка при отсеивании слишком старых объявлений: {err}")
        return ads


def filter_by_reserve(ads: AdList, ctx: FilterContext) -> AdList:
    if not ctx.config.ignore_reserv:
        return ads
    try:
        return [ad for ad in ads if not ad.isReserved]
    except Exception as err:
        logger.debug(f"Ошибка при отсеивании объявлений в резерве: {err}")
        return ads


def filter_by_promotion(ads: AdList, ctx: FilterContext) -> AdList:
    ads = _add_promotion_flag(ads)
    if not ctx.config.ignore_promotion:
        return ads
    try:
        return [ad for ad in ads if not ad.isPromotion]
    except Exception as err:
        logger.debug(f"Ошибка при отсеивании продвинутых объявлений: {err}")
        return ads


FILTERS_ORDER = [
    filter_viewed,
    filter_by_price_range,
    filter_by_black_keywords,
    filter_by_white_keywords,
    filter_by_address,
    filter_by_seller,
    filter_by_recent_time,
    filter_by_reserve,
    filter_by_promotion,
]


def apply_filters(ads: AdList, ctx: FilterContext) -> AdList:
    """
    Применяет все фильтры по очереди и логирует, сколько объявлений осталось.
    """
    for filter_fn in FILTERS_ORDER:
        ads = filter_fn(ads, ctx)
        logger.info(f"После фильтрации {filter_fn.__name__} осталось {len(ads)}")
        if not ads:
            break
    return ads
