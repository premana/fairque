"""Function task example showing how to use @fairque.task decorator."""

from fairque import FairQueueConfig, Priority, TaskHandler, TaskQueue, task


@task(max_retries=2)
def fib(n: int) -> int:
    """Fibonacci function decorated as FairQueue task."""
    if n <= 1:
        return 1
    else:
        return fib_slow(n - 1) + fib_slow(n - 2)


def fib_slow(n: int) -> int:
    """Non-decorated fibonacci for recursive calls."""
    if n <= 1:
        return 1
    else:
        return fib_slow(n - 1) + fib_slow(n - 2)


@task(priority=Priority.HIGH, max_retries=1)
def greet(name: str, greeting: str = "Hello") -> str:
    """Greeting function with keyword arguments."""
    return f"{greeting}, {name}!"


@task(priority=Priority.CRITICAL)
def process_data(data: dict) -> dict:
    """Data processing task."""
    processed = {}
    for key, value in data.items():
        processed[f"processed_{key}"] = value * 2
    return processed


class SimpleTaskHandler(TaskHandler):
    """Simple task handler for demonstration."""

    def _process_task(self, task) -> bool:
        """Custom task processing (only called if task.func is None)."""
        print(f"Processing custom task: {task.task_id}")
        # Custom logic here
        return True


def main():
    """Demonstrate function task usage."""
    print("=== FairQueue Function Task Example ===\n")

    # Create configuration
    config = FairQueueConfig.create_development()

    # Create task handler and queue
    handler = SimpleTaskHandler()

    with TaskQueue(config) as queue:
        # Example 1: Create task with decorator
        print("1. Creating fibonacci task...")
        fib_task = fib(10)
        print(f"   Task ID: {fib_task.task_id}")
        print(f"   Function: {fib_task.func.__name__}")
        print(f"   Arguments: {fib_task.args}")

        # Execute function directly
        print("   Executing function directly...")
        result = fib_task()
        print(f"   Result: {result}")

        # Push to queue
        print("   Pushing to queue...")
        queue.push(fib_task)

        print()

        # Example 2: Greeting task with keywords
        print("2. Creating greeting task...")
        greet_task = greet("World", greeting="Hi")
        print(f"   Task ID: {greet_task.task_id}")
        print(f"   Arguments: {greet_task.args}")
        print(f"   Keyword arguments: {greet_task.kwargs}")

        # Execute and push
        result = greet_task()
        print(f"   Result: {result}")
        queue.push(greet_task)

        print()

        # Example 3: Data processing task
        print("3. Creating data processing task...")
        data = {"a": 1, "b": 2, "c": 3}
        process_task = process_data(data)

        result = process_task()
        print(f"   Input: {data}")
        print(f"   Result: {result}")
        queue.push(process_task)

        print()

        # Example 4: Task argument modification
        print("4. Modifying task arguments...")
        base_greet = greet("Alice")
        print(f"   Original: {base_greet.args}, {base_greet.kwargs}")

        # Create new task with different arguments
        modified_greet = base_greet("Bob", greeting="Hey")
        print(f"   Modified: {modified_greet.args}, {modified_greet.kwargs}")

        # Both tasks are independent
        result1 = base_greet()
        result2 = modified_greet()
        print(f"   Results: '{result1}' vs '{result2}'")

        print()

        # Example 5: Queue statistics
        print("5. Queue statistics:")
        stats = queue.get_batch_queue_sizes()
        total_tasks = stats.get("totals", {}).get("total_size", 0)
        print(f"   Total tasks in queue: {total_tasks}")

        print("\n=== Example Complete ===")


if __name__ == "__main__":
    main()
