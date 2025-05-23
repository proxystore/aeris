# Academy: Federated Actors and Agents

![PyPI - Version](https://img.shields.io/pypi/v/academy-py)
[![tests](https://github.com/proxystore/academy/actions/workflows/tests.yml/badge.svg)](https://github.com/proxystore/academy/actions)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/proxystore/academy/main.svg)](https://results.pre-commit.ci/latest/github/proxystore/academy/main)

Academy is a modular and extensible middleware for building and deploying stateful actors and autonomous agents across distributed systems and federated research infrastructure.
In Academy, you can:

* ⚙️  Express agent behavior and state in code
* 📫 Manage inter-agent coordination and asynchronous communication
* 🌐 Deploy agents across distributed, federated, and heterogeneous resources

## Installation

Academy is available on [PyPI](https://pypi.org/project/academy-py/).

```bash
pip install academy-py
```

## Example

Agents in Academy are defined by a `Behavior`, a class with methods decorated with `@action` can be invoked by peers and method decorated with `@loop` run autonomous control loops.

The below sensor monitoring behavior periodically reads a sensor in the `monitor()` loop and processes the reading if a threshold is met.
Clients or peers can invoke the `get_last_reading()` and `set_process_threshold()` actions remotely to interact with the monitor agent.

```python
import time, threading
from academy.behavior import Behavior, action, loop

class SensorMonitorAgent(Behavior):
    def __init__(self) -> None:
        self.last_reading: float | None = None
        self.process_threshold: float = 1.0

    @action
    def get_last_reading(self) -> float | None:
        return self.last_reading

    @action
    def set_process_threshold(self, value: float) -> None:
        self.process_threshold = value

    @loop
    def monitor(self, shutdown: threading.Event) -> None:
        while not shutdown.is_set():
            value = read_sensor_data()
            self.last_reading = value:
            if value >= self.process_threshold:
                process_reading(value)
            time.sleep(1)
```

Entities communicate asynchronously through *handles*, sending messages to and receiving messages from a mailbox managed by an `Exchange`.
The `Launcher` abstracts the remote execution of an agent, and the `Manager` provides easy management of handles, launchers, and the exchange.

```python
from academy.exchange.thread import ThreadExchange
from academy.launcher import ThreadLauncher
from academy.manager import Manager

with Manager(
    exchange=ThreadExchange(),  # Replace with other implementations
    launcher=ThreadLauncher(),  # for distributed deployments
) as manager:
    behavior = SensorMonitorAgent()  # From the above block
    agent_handle = manager.launch(behavior)

    agent_handle.set_process_threshold(2.0).result()
    time.sleep(5)
    value = agent_handle.get_last_reading().result()

    manager.shutdown(handle, blocking=True)
```

Learn more about Academy in [Getting Started](https://academy.proxystore.dev/latest/get-started).

## What can be an agent?

In Academy, an agent is a primitive entity that (1) has internal state, (2) performs actions, and (3) communicates with other agents.

This allows for range of agent implementations—Academy agents are building blocks for constructing more complex agent-based systems.

For example, Academy can be use to create the following:

* **Stateful Actors:** Actors manage their own data and respond to requests in a distributed system.
* **LLM Agents:** Integrate LLM-based reasoning and tool calling.
* **Embodied Agents:** The "brain" controlling a robot or simulated entity where action are translated into motor commands or environment manipulations.
* **Computational Units:** Encapsulate a specific computational task, like running a simulation, processing data, or training a machine learning model.
* **Orchestrators:** Manage or coordinate the activities of other agents, distributing tasks and monitoring progress.
* **Data Interfaces:** Interact with external data sources, such as databases, file systems, or sensors, providing a consistent interface for data access and manipulation.

## Why Academy?

Academy offers a powerful and flexible framework for building sophisticated, distributed agent-based systems, particularly well-suited for the complexities of scientific applications.
Here's what makes Academy valuable:

* **Stateful Agents:** Academy enables agents to maintain state, which is crucial for managing long-running processes, tracking context across steps, and implementing agents that need to "remember" information.
* **Agent Autonomy:** Academy allows agents to have autonomous control loops, empowering them to make decisions, react to events, and execute tasks independently.
* **Flexible Deployment:** Academy provides tools for managing agent deployment, communication, and coordination in complex environments such that applications can leverage heterogeneous, distributed, and federated resources.
* **Foundation for Sophisticated Applications:** Academy primitives offer a strong foundation for building highly specialized and sophisticated agent-based systems that go beyond standard LLM use cases, allowing for fine-grained control and optimization tailored to specific scientific applications.

## Citation

The Academy preprint is available on [arXiv](https://arxiv.org/abs/2505.05428).

```bibtex
@misc{pauloski2025academy,
    title = {{E}mpowering {S}cientific {W}orkflows with {F}ederated {A}gents},
    author = {J. Gregory Pauloski and Yadu Babuji and Ryan Chard and Mansi Sakarvadia and Kyle Chard and Ian Foster},
    archivePrefix = {arXiv},
    eprint = {2505.05428},
    primaryClass = {cs.MA},
    url = {https://arxiv.org/abs/2505.05428},
    year = {2025},
}
```
