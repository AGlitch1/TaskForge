import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any


LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


FILE_LOG_FORMAT = (
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

CONSOLE_LOG_FORMAT = "%(levelname)s : %(message)s"


class ExtraFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)

        extra_fields = getattr(record, "extra_fields", None)

        if isinstance(extra_fields, dict) and extra_fields:
            extra_string = " ".join(
                f"{key}={value}" for key, value in extra_fields.items()
            )
            message = f"{message} - {extra_string}"

        return message


def configure_logging(
    *,
    service_name: str,
    log_to_console: bool = True,
) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    file_formatter = ExtraFormatter(FILE_LOG_FORMAT)

    file_handler = RotatingFileHandler(
        LOG_DIR / f"{service_name}.log",
        maxBytes=5_000_000,
        backupCount=5,
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)

    if log_to_console:
        console_formatter = logging.Formatter(CONSOLE_LOG_FORMAT)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_extra(**kwargs: Any) -> dict[str, dict[str, Any]]:
    return {
        "extra_fields": kwargs,
    }