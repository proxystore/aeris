# Quick Start

## Installation

```bash
git clone https://github.com/proxystore/taps
cd taps
python -m venv venv
. venv/bin/activate
pip install -e .
```

Documentation on installing for local development is provided in [Contributing](contributing/index.md).

## Usage

Applications can be executed from the CLI.
```bash
python -m taps.run --app {name} {args}
```
See `python -m taps.run --help` for a list of applications.
