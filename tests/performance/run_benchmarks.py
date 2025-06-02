"""Run all performance tests."""

import argparse
import sys

import pytest


def main():
    """Main entry point for performance tests."""
    parser = argparse.ArgumentParser(description="Run FairQueue performance tests")
    parser.add_argument(
        "--test",
        "-t",
        choices=["queue", "worker", "redis", "async", "all"],
        default="all",
        help="Which performance tests to run"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--capture",
        "-c",
        choices=["yes", "no", "sys"],
        default="no",
        help="Capture output"
    )

    args = parser.parse_args()

    # Build pytest arguments
    pytest_args = ["tests/performance/"]

    if args.verbose:
        pytest_args.append("-v")

    pytest_args.append(f"--capture={args.capture}")

    # Add specific test file based on choice
    if args.test != "all":
        test_files = {
            "queue": "test_queue_throughput.py",
            "worker": "test_worker_performance.py",
            "redis": "test_redis_operations.py",
            "async": "test_async_performance.py",
        }
        pytest_args.append(f"tests/performance/{test_files[args.test]}")

    # Run pytest
    sys.exit(pytest.main(pytest_args))


if __name__ == "__main__":
    main()
