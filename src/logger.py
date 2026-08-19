"""애플리케이션 전역 로거 설정.

Docker(비-TTY) 환경에서 print()의 내부 버퍼링과 로깅 핸들러의 flush 타이밍이
어긋나 로그 순서가 뒤섞이는 문제를 막기 위해, src/ 전역에서 print() 대신
이 모듈의 logger를 사용합니다.
"""
import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(name: str = "ceiling_tracker") -> logging.Logger:
    """애플리케이션 전역 로거를 초기화하고 반환합니다."""
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)

    log_format = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    try:
        log_dir = os.path.join(os.getenv("LOCAL_STORAGE_BASE_PATH", "data"), "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "app.log")

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8"
        )
        file_handler.setFormatter(log_format)
        logger.addHandler(file_handler)
    except Exception as e:
        console_handler.setLevel(logging.WARNING)
        logger.warning(f"Failed to setup file logging: {e}")

    return logger


logger = setup_logger()


if __name__ == '__main__':
    # ponytail: 콘솔+파일 핸들러가 둘 다 붙는지 확인하는 self-check
    assert len(logger.handlers) == 2
    logger.info("[logger] self-check OK")
