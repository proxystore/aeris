"""Example script that uses the foobar package."""

from __future__ import annotations

import argparse

from foobar.foo import bar


def main() -> int:
    """Script entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-n',
        '--numbers',
        nargs='+',
        type=int,
        default=None,
        help='List of integers to sum',
    )
    args = parser.parse_args()

    result = bar(args.numbers)

    print(f'Input: {args.numbers}')
    print(f'Result: {result}')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
