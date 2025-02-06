from __future__ import annotations

import logging
import pathlib
import sys

LOG_FORMAT = (
    '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) > %(message)s'
)


def init_logging(
    level: int | str = logging.INFO,
    *,
    logfile: str | pathlib.Path | None = None,
    force: bool = False,
) -> None:
    """Initialize global logger.

    Args:
        level: Minimum logging level.
        logfile: Configure a file handler for this path.
        force: Remove any existing handlers attached to the root
            handler. This option is useful to silencing the third-party
            package logging. Note: should not be set when running inside
            pytest.
    """
    path = pathlib.Path(logfile) if logfile is not None else None
    stdout_handler = logging.StreamHandler(sys.stdout)
    handlers: list[logging.Handler] = [stdout_handler]
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path)
        handlers.append(handler)

    logging.basicConfig(
        format=LOG_FORMAT,
        datefmt='%Y-%m-%d %H:%M:%S',
        level=level,
        handlers=handlers,
        force=force,
    )

    # This needs to be after the configuration of the root logger because
    # warnings get logged to a 'py.warnings' logger.
    logging.captureWarnings(True)

    logging.info(
        'Configured logger (level=%s, file=%s)',
        logging.getLevelName(level) if isinstance(level, int) else level,
        path,
    )
