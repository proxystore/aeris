# Academy: Build and Deploy Federated Multi-Agent Systems

[![tests](https://github.com/proxystore/academy/actions/workflows/tests.yml/badge.svg)](https://github.com/proxystore/academy/actions)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/proxystore/academy/main.svg)](https://results.pre-commit.ci/latest/github/proxystore/academy/main)

Academy is a modular and extensible middleware for building and deploying autonomous agents across federated research infrastructure.
In Academy, you can:
* ⚙️ Express agent behavior and state in code
* 📫 Manage inter-agent coordination and asynchronous communication
* 🌐 Deploy agents across distributed, federated, and heterogeneous resources

> [!IMPORTANT]
> This project is currently being developed. Expect breaking changes in minor releases.

## Installation

Academy is available on [PyPI][https://pypi.org/project/academy-py/].

```bash
pip install academy-py
```

<details>
<summary>Install from Source</summary>
<br>
**Clone the source:**
```bash
git clone git@github.com:proxystore/academy
```

**Create a virtual environment of your choosing:**
```bash
python -m venv venv
. venv/bin/activate
```

**Install the package & dependencies:**
```bash
pip install -e .  # -e for editable mode
```
</details>

## Getting Started

An agent is defined by its behavior, implemented as a Python class basing `Behavior`.
Behavior methods decorated as `@action` can be invoked by peer agents while `@loop` methods define autonomous control loops.
Any number of actions or control loops can be defined on a `Behavior`.

```python
import time, threading
from academy.behavior import Behavior, action, loop

class Example(Behavior):
    def __init__(self) -> None:
        self.count = 0  # State stored as attributes

    @action
    def square(self, value: float) -> float:
        return value**2

    @loop
    def count(self, shutdown: threading.Event) -> None:
        while not shutdown.is_set():
            self.count += 1
            time.sleep(1)
```

Agents communicate asynchronously through *handles*, sending messages to and receiving messages from a mailbox stored in an `Exchange`.
The `Launcher` abstracts the remote execution of an agent, and the `Manager` provides easy management of handles, launchers, and the exchange.

```python
from academy.exchange.thread import ThreadExchange
from academy.launcher.thread import ThreadLauncher
from academy.manager import Manager

with Manager(
    exchange=ThreadExchange(),  # Can be swapped with
    launcher=ThreadLauncher(),  # other implementations
) as manager:
    behavior = Example()  # From the above block
    handle = manager.launch(behavior)

    future = handle.square(2)
    assert future.result() == 4

    handle.shutdown()  # Or via the manager
    manager.shutdown(handle, blocking=True)
```
