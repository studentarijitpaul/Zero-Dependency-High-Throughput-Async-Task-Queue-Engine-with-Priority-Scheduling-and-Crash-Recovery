from pathlib import Path


PROJECT_ROOT = Path(__file__).parent


DIRECTORIES = [
    "src/async_task_queue",
    "src/async_task_queue/task",
    "src/async_task_queue/queue",
    "src/async_task_queue/worker",
    "src/async_task_queue/retry",
    "src/async_task_queue/persistence",
    "tests",
    "tests/unit",
    "tests/unit/task",
    "tests/unit/queue",
    "tests/unit/worker",
    "tests/unit/retry",
    "tests/unit/persistence",
    "tests/integration",
    "examples",
    "docs",
    ".github/workflows",
]


FILES = [
    # Package
    "src/async_task_queue/__init__.py",
    "src/async_task_queue/exceptions.py",
    "src/async_task_queue/protocols.py",
    "src/async_task_queue/types.py",

    # Task
    "src/async_task_queue/task/__init__.py",
    "src/async_task_queue/task/model.py",
    "src/async_task_queue/task/state.py",
    "src/async_task_queue/task/registry.py",

    # Queue
    "src/async_task_queue/queue/__init__.py",
    "src/async_task_queue/queue/queue.py",
    "src/async_task_queue/queue/scheduler.py",

    # Worker
    "src/async_task_queue/worker/__init__.py",
    "src/async_task_queue/worker/worker.py",

    # Retry
    "src/async_task_queue/retry/__init__.py",
    "src/async_task_queue/retry/policy.py",

    # Persistence
    "src/async_task_queue/persistence/__init__.py",
    "src/async_task_queue/persistence/protocol.py",
    "src/async_task_queue/persistence/serializer.py",
    "src/async_task_queue/persistence/json_store.py",

    # Unit tests
    "tests/unit/task/test_model.py",
    "tests/unit/task/test_state.py",
    "tests/unit/task/test_registry.py",
    "tests/unit/queue/test_scheduler.py",
    "tests/unit/queue/test_queue.py",
    "tests/unit/worker/test_worker.py",
    "tests/unit/retry/test_policy.py",
    "tests/unit/persistence/test_serializer.py",
    "tests/unit/persistence/test_json_store.py",

    # Integration tests
    "tests/integration/test_queue_execution.py",
    "tests/integration/test_retry_flow.py",
    "tests/integration/test_persistence_flow.py",
    "tests/integration/test_recovery.py",
    "tests/integration/test_shutdown.py",

    # Test configuration
    "tests/conftest.py",

    # Examples
    "examples/basic_queue.py",
    "examples/priorities.py",
    "examples/retries.py",
    "examples/persistence.py",

    # Documentation
    "docs/architecture.md",
    "docs/task-lifecycle.md",
    "docs/concurrency.md",
    "docs/persistence.md",
    "docs/design-decisions.md",

    # GitHub Actions
    ".github/workflows/ci.yml",
    ".github/workflows/release.yml",

    # Root files
    ".gitignore",
    "LICENSE",
    "README.md",
    "CHANGELOG.md",
    "pyproject.toml",
]


def create_directories() -> None:
    """Create all project directories."""
    for directory in DIRECTORIES:
        path = PROJECT_ROOT / directory
        path.mkdir(parents=True, exist_ok=True)


def create_files() -> None:
    """Create all project files if they do not already exist."""
    for file in FILES:
        path = PROJECT_ROOT / file
        path.parent.mkdir(parents=True, exist_ok=True)

        if not path.exists():
            path.touch()


def main() -> None:
    """Build the project structure."""
    create_directories()
    create_files()

    print("Project structure created successfully.")


if __name__ == "__main__":
    main()