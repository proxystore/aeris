from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future

from aeris.behavior import action
from aeris.behavior import Behavior
from aeris.behavior import loop
from aeris.exchange.thread import ThreadExchange
from aeris.launcher.thread import ThreadLauncher
from aeris.logging import init_logging
from aeris.manager import Manager


class Counter(Behavior):
    count: int

    def setup(self) -> None:
        self.count = 0

    @loop
    def increment(self, shutdown: threading.Event) -> None:
        while not shutdown.is_set():
            time.sleep(1)
            self.count += 1

    @action
    def get_count(self) -> int:
        return self.count


def main() -> int:
    init_logging(logging.DEBUG)

    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),
    ) as manager:
        behavior = Counter()
        agent = manager.launch(behavior)

        time.sleep(2)

        future: Future[int] = agent.action('get_count')
        assert future.result() >= 1

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
