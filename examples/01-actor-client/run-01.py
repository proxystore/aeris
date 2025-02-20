from __future__ import annotations

import logging
from concurrent.futures import Future

from aeris.behavior import action
from aeris.behavior import Behavior
from aeris.exchange.thread import ThreadExchange
from aeris.launcher.thread import ThreadLauncher
from aeris.logging import init_logging
from aeris.manager import Manager


class Counter(Behavior):
    count: int

    def on_setup(self) -> None:
        self.count = 0

    @action
    def increment(self, value: int = 1) -> None:
        self.count += value

    @action
    def get_count(self) -> int:
        return self.count


def main() -> int:
    init_logging(logging.INFO)

    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),
    ) as manager:
        behavior = Counter()
        agent = manager.launch(behavior)

        future: Future[int] = agent.action('get_count')
        assert future.result() == 0

        agent.action('increment').result()

        future = agent.action('get_count')
        assert future.result() == 1

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
