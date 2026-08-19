"""src/logger.py 유닛 테스트"""
import logging
from unittest.mock import patch

from src.logger import setup_logger


def test_file_handler_failure_does_not_mute_console_info_logs():
    """파일 핸들러 설정이 실패해도 콘솔 핸들러는 INFO 레벨을 유지해야 합니다.

    (회귀 테스트: 예전엔 except 블록이 console_handler.setLevel(WARNING)을
    호출해서, 파일 로깅 실패 이후 모든 info() 호출이 콘솔에서도 조용히
    사라졌음.)
    """
    with patch("src.logger.RotatingFileHandler", side_effect=PermissionError("denied")):
        logger = setup_logger(name="test_logger_file_handler_failure")

    console_handler = logger.handlers[0]
    assert console_handler.level in (logging.NOTSET, logging.INFO)
