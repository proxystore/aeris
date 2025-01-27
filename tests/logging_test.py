from __future__ import annotations

import logging
import pathlib

from aeris.logging import init_logging

# Note: these tests are just for coverage to make sure the code is functional.
# It does not test the behavior of init_logging because pytest captures
# logging already.


def test_logging_no_file() -> None:
    init_logging()

    logger = logging.getLogger()
    logger.info('Test logging')


def test_logging_with_file(tmp_path: pathlib.Path) -> None:
    filepath = tmp_path / 'log.txt'
    init_logging(logfile=filepath)

    logger = logging.getLogger()
    logger.info('Test logging')
