"""Example demonstrating FairQueue pipeline and task group functionality."""

import time

from fairque.core.config import FairQueueConfig
from fairque.core.models import Priority, Task
from fairque.decorator import task
from fairque.queue.queue import TaskQueue


# Define tasks using decorators with custom IDs
@task(task_id="extract", priority=Priority.HIGH)
def extract_data():
    """Extract data from source."""
    print("Extracting data...")
    time.sleep(1)
    return {"data": "raw_data", "records": 1000}


@task(task_id="transform", priority=Priority.NORMAL)
def transform_data():
    """Transform extracted data."""
    print("Transforming data...")
    time.sleep(2)
    return {"data": "transformed_data", "records": 1000}


@task(task_id="validate", priority=Priority.NORMAL)
def validate_data():
    """Validate transformed data."""
    print("Validating data...")
    time.sleep(1)
    return {"validation": "passed", "errors": 0}


@task(task_id="load", priority=Priority.LOW)
def load_data():
    """Load data to destination."""
    print("Loading data...")
    time.sleep(1.5)
    return {"status": "loaded", "records": 1000}


@task(task_id="notify", priority=Priority.HIGH)
def send_notification():
    """Send completion notification."""
    print("Sending notification...")
    time.sleep(0.5)
    return {"notification": "sent"}


def main():
    """Demonstrate various pipeline patterns."""

    # Create configuration
    config = FairQueueConfig.create_default(
        worker_id="pipeline-worker",
        assigned_users=["user:pipeline"],
    )

    with TaskQueue(config) as queue:
        print("=== FairQueue Pipeline Examples ===\n")

        # Example 1: Simple linear pipeline
        print("1. Simple Linear Pipeline: extract >> transform >> load")

        task1 = extract_data()
        task2 = transform_data()
        task3 = load_data()

        # Create pipeline using >> operator
        linear_pipeline = task1 >> task2 >> task3

        # Enqueue the pipeline
        results = queue.enqueue(linear_pipeline)
        print(f"Enqueued {len(results)} tasks from linear pipeline\n")

        # Example 2: Parallel validation and transform
        print("2. Parallel Processing: extract >> (transform | validate) >> load")

        task1 = extract_data()
        task2 = transform_data()
        task3 = validate_data()
        task4 = load_data()

        # Create parallel group using | operator
        parallel_pipeline = task1 >> (task2 | task3) >> task4

        results = queue.enqueue(parallel_pipeline)
        print(f"Enqueued {len(results)} tasks from parallel pipeline\n")

        # Example 3: Complex workflow with notification
        print("3. Complex Workflow: ETL + Validation + Notification")

        # Create tasks
        extract = extract_data()
        transform = transform_data()
        validate = validate_data()
        load = load_data()
        notify = send_notification()

        # Build complex pipeline
        # extract >> transform >> (validate | load) >> notify
        complex_pipeline = extract >> transform >> (validate | load) >> notify

        results = queue.enqueue(complex_pipeline)
        print(f"Enqueued {len(results)} tasks from complex workflow\n")

        # Example 4: Using reverse operator
        print("4. Reverse Dependencies: notify << load << transform")

        notify_task = send_notification()
        load_task = load_data()
        transform_task = transform_data()

        # Using << operator (reverse dependency)
        reverse_pipeline = notify_task << load_task << transform_task

        results = queue.enqueue(reverse_pipeline)
        print(f"Enqueued {len(results)} tasks from reverse pipeline\n")

        # Example 5: Mixed operators
        print("5. Mixed Operators: Complex ETL with parallel validation")

        e = extract_data()
        t1 = transform_data()
        t2 = Task.create(
            task_id="transform_alt",
            func=lambda: {"data": "alt_transform", "method": "alternative"},
            user_id="user:pipeline"
        )
        v = validate_data()
        l = load_data()
        n = send_notification()

        # Complex pipeline: extract >> (transform1 | transform2) >> validate >> load >> notify
        mixed_pipeline = e >> (t1 | t2) >> v >> l >> n

        results = queue.enqueue(mixed_pipeline)
        print(f"Enqueued {len(results)} tasks from mixed operator pipeline\n")

        print("=== All pipelines enqueued successfully! ===")
        print("Check your worker logs to see execution order.")


if __name__ == "__main__":
    main()