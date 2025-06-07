# Fair Queue Implementation with Redis 🚀

![Fair Queue](https://img.shields.io/badge/Fair_Queue-Redis-blue.svg) ![Python](https://img.shields.io/badge/Python-3.8%2B-green.svg) ![Asyncio](https://img.shields.io/badge/Asyncio-Enabled-orange.svg) ![Releases](https://img.shields.io/badge/Releases-Check%20Here-brightgreen.svg)

Welcome to the **Fair Queue** repository! This project offers a robust implementation of a fair queue system using Redis. It features work stealing and priority scheduling, making it an excellent choice for managing tasks in a multi-tenant environment.

## Table of Contents

1. [Introduction](#introduction)
2. [Features](#features)
3. [Getting Started](#getting-started)
4. [Installation](#installation)
5. [Usage](#usage)
6. [Configuration](#configuration)
7. [Examples](#examples)
8. [Contributing](#contributing)
9. [License](#license)
10. [Releases](#releases)

## Introduction

In today's fast-paced world, efficient task management is crucial. Fair Queue leverages Redis to ensure tasks are processed fairly and efficiently. This implementation supports asynchronous operations, making it suitable for applications that require high performance and scalability.

## Features

- **Fair Scheduling**: Tasks are processed in a manner that ensures fairness across all tenants.
- **Work Stealing**: Idle workers can take on tasks from busy workers, improving resource utilization.
- **Priority Scheduling**: Tasks can be assigned different priority levels to manage critical workloads effectively.
- **Multi-Tenant Support**: Designed to handle multiple tenants seamlessly, ensuring isolation and security.
- **Easy Integration**: Built with Python and Redis, making it easy to integrate into existing systems.

## Getting Started

To start using Fair Queue, you need to have Python and Redis installed. This section will guide you through the installation and basic usage.

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/premana/fairque.git
   cd fairque
   ```

2. **Install Dependencies**:
   Use pip to install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run Redis**:
   Ensure you have Redis running on your machine. You can start it using:
   ```bash
   redis-server
   ```

## Usage

Once you have everything set up, you can start using Fair Queue. Here’s a simple example of how to add tasks to the queue and process them.

### Adding Tasks

You can add tasks to the queue by calling the appropriate function. Here’s a sample code snippet:

```python
from fairque import FairQueue

queue = FairQueue()

# Adding tasks
queue.add_task('task_1', priority=1)
queue.add_task('task_2', priority=2)
```

### Processing Tasks

To process tasks, simply call the worker function. This will start consuming tasks from the queue.

```python
queue.start_worker()
```

## Configuration

Fair Queue can be configured to suit your needs. You can adjust settings such as the number of workers, task timeout, and priority levels. Here’s how to configure:

```python
queue.configure(workers=5, timeout=30)
```

## Examples

### Basic Example

Here’s a complete example that demonstrates adding and processing tasks:

```python
from fairque import FairQueue

def main():
    queue = FairQueue()
    queue.add_task('task_1', priority=1)
    queue.add_task('task_2', priority=2)
    queue.start_worker()

if __name__ == "__main__":
    main()
```

### Advanced Example with Work Stealing

This example shows how to implement work stealing:

```python
from fairque import FairQueue

def worker_function():
    # Your task processing logic here
    pass

queue = FairQueue()
queue.start_worker(worker_function)
```

## Contributing

We welcome contributions to Fair Queue! If you want to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and commit them.
4. Push to your fork and submit a pull request.

Please ensure that your code follows the existing style and includes tests where applicable.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Releases

To download the latest version, visit the [Releases](https://github.com/premana/fairque/releases) section. Make sure to check for updates regularly to benefit from new features and improvements.

Thank you for checking out Fair Queue! If you have any questions or feedback, feel free to reach out.