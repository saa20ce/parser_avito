from __future__ import annotations

from loguru import logger


class ParseLinksUseCase:
    """
    Use-case: обойти все ссылки из конфигурации и запустить парсинг.

    Ничего не знает про детали реализации, работает с "парсером",
    у которого есть:
      - config
      - xlsx_handler
      - http_client
      - tg_handler
      - stop_event
      - _should_stop()
      - _process_url(index, url)
      - _save_data(ads)
    """

    def __init__(self, parser) -> None:
        self.parser = parser

    def run(self) -> None:
        cfg = self.parser.config

        if cfg.one_file_for_link:
            self.parser.xlsx_handler = None

        for index, url in enumerate(cfg.urls):
            if self.parser._should_stop():
                break

            ads_in_link = self.parser._process_url(index=index, url=url)

            if ads_in_link and self.parser.xlsx_handler and not cfg.one_file_for_link:
                logger.info(f"Сохраняю в Excel {len(ads_in_link)} объявлений")
                self.parser._save_data(ads_in_link)
            elif not ads_in_link:
                logger.info("Сохранять нечего")

            if cfg.one_file_for_link:
                self.parser.xlsx_handler = None

        logger.info(
            f"Хорошие запросы: {self.parser.http_client.good_request_count}шт, "
            f"плохие: {self.parser.http_client.bad_request_count}шт"
        )

        if cfg.one_time_start and self.parser.tg_handler:
            self.parser.tg_handler.send_to_tg(
                msg="Парсинг Авито завершён. Все ссылки обработаны"
            )
            if self.parser.stop_event:
                self.parser.stop_event.set()
