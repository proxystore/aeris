from __future__ import annotations

import logging
from concurrent.futures import Future

from aeris.behavior import action
from aeris.behavior import BehaviorMixin
from aeris.exchange.thread import ThreadExchange
from aeris.launcher.thread import ThreadLauncher


class Counter(BehaviorMixin):
    count: int

    def setup(self) -> None:
        self.count = 0

    @action
    def increment(self, value: int = 1) -> None:
        self.count += value

    @action
    def get_count(self) -> int:
        return self.count


def main() -> int:
    logging.basicConfig(level=logging.DEBUG)

    behavior = Counter()
    exchange = ThreadExchange()

    with ThreadLauncher(exchange) as launcher:
        agent = launcher.launch(behavior)

        future: Future[int] = agent.action('get_count')
        assert future.result() == 0

        agent.action('increment').result()

        future = agent.action('get_count')
        assert future.result() == 1

        agent.close()

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
