import logging
from logging import handlers
import pathlib


LOG_DIR = pathlib.Path("logs")
LOG_FILE = LOG_DIR / "etl.log"


def setup_logging() -> None:
    """統一設定 Console 與 File Logging。"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger = logging.getLogger("trade_analytics")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.info("日誌已啟用：%s", LOG_FILE.resolve())
