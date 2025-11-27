from dataclasses import dataclass, field
from typing import Protocol, Sequence, List

from avito_parser.core.models import Item
from avito_parser.infrastructure.db_service import SQLiteDBHandler
from avito_parser.infrastructure.tg_sender import SendAdToTg


class ViewedRepository(Protocol):
    def mark_viewed(self, ads: Sequence[Item]) -> None: ...


class Notifier(Protocol):
    def notify_ads(self, ads: Sequence[Item]) -> None: ...


class SQLiteViewedRepository(ViewedRepository):
    """Адаптер над SQLiteDBHandler для записи просмотренных объявлений."""

    def __init__(self, db: SQLiteDBHandler):
        self._db = db

    def mark_viewed(self, ads: Sequence[Item]) -> None:
        self._db.add_record_from_page(list(ads))


class TelegramNotifier(Notifier):
    """
    Адаптер над SendAdToTg.
    Учитывает флаг one_time_start: если он включен — объявления не рассылаются.
    """

    def __init__(self, sender: SendAdToTg, one_time_start: bool = False):
        self._sender = sender
        self._one_time_start = one_time_start

    def notify_ads(self, ads: Sequence[Item]) -> None:
        if self._one_time_start:
            return

        for ad in ads:
            self._sender.send_to_tg(ad=ad)


@dataclass
class AdProcessor:
    """
    Отвечает за побочные эффекты поверх уже отфильтрованных объявлений:
    - отметить в БД как просмотренные
    - разослать уведомления (Telegram, и в будущем — другие каналы)
    """

    viewed_repo: ViewedRepository | None = None
    notifiers: List[Notifier] = field(default_factory=list)

    def process(self, ads: list[Item]) -> None:
        if not ads:
            return

        if self.viewed_repo:
            self.viewed_repo.mark_viewed(ads)

        for notifier in self.notifiers:
            notifier.notify_ads(ads)
