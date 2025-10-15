"""
Worker implementation for distributed MapReduce.
Executes map and reduce tasks assigned by the coordinator.
"""

import os
import sys
import time
import uuid
import importlib.util
import tempfile
from typing import List, Optional, Any

from .mapreduce import (
    Task,
    TaskType,
    KeyValue,
    MapReduceApp,
    ihash,
    intermediate_filename,
    output_filename,
    read_key_values_from_file,
    write_key_values_to_file,
    write_final_output,
    sort_key_values,
    group_by_key,
)
from .rpc_utils import RPCClient


class Worker:
    """
    Worker process for distributed MapReduce.
    Requests tasks from coordinator and executes map/reduce operations.
    """

    def __init__(self, map_reduce_app: MapReduceApp):
        """
        Initialize worker with a MapReduce application.

        Args:
            map_reduce_app: The MapReduce application to run
        """
        self.worker_id = str(uuid.uuid4())
        self.map_reduce_app = map_reduce_app
        self.rpc_client: Optional[RPCClient] = None

    def run(self):
        """Main worker loop."""
        # Connect to coordinator
        if not self._connect_to_coordinator():
            print("Failed to connect to coordinator")
            return

        print(f"Worker {self.worker_id} started")

        # Main work loop
        while True:
            # Request task from coordinator
            task = self._request_task()

            if task is None:
                print("Failed to get task from coordinator")
                break

            if task.task_type == TaskType.EXIT:
                print("Received exit signal from coordinator")
                break

            if task.task_type == TaskType.WAIT:
                # No task available, wait and try again
                time.sleep(1)
                continue

            # Execute the task
            print(f"Executing {task.task_type.value} task {task.task_id}")
            success = self._execute_task(task)

            # Report completion to coordinator
            self._report_completion(task.task_id, success)

    def _connect_to_coordinator(self) -> bool:
        """Connect to the coordinator using info from file."""
        try:
            with open("coordinator_info.txt", "r") as f:
                coordinator_info = f.read().strip()
                host, port = coordinator_info.split(":")
                port = int(port)

            self.rpc_client = RPCClient(host=host, port=port)
            return True

        except Exception as e:
            print(f"Failed to connect to coordinator: {e}")
            return False

    def _request_task(self) -> Optional[Task]:
        """Request a task from the coordinator."""
        if self.rpc_client is None:
            return None

        try:
            response = self.rpc_client.call("request_task", worker_id=self.worker_id)
            if response is None:
                return None

            return Task.from_dict(response)

        except Exception as e:
            print(f"Error requesting task: {e}")
            return None

    def _report_completion(self, task_id: int, success: bool, error_message: str = ""):
        """Report task completion to coordinator."""
        if self.rpc_client is None:
            return

        try:
            self.rpc_client.call(
                "complete_task",
                worker_id=self.worker_id,
                task_id=task_id,
                success=success,
                error_message=error_message,
            )
        except Exception as e:
            print(f"Error reporting completion: {e}")

    def _execute_task(self, task: Task) -> bool:
        """Execute a specific task."""
        try:
            if task.task_type == TaskType.MAP:
                return self._execute_map_task(task)
            elif task.task_type == TaskType.REDUCE:
                return self._execute_reduce_task(task)
            else:
                print(f"Unknown task type: {task.task_type}")
                return False

        except Exception as e:
            print(f"Error executing task {task.task_id}: {e}")
            return False

    def _execute_map_task(self, task: Task) -> bool:
        """Execute a map task."""
        try:
            # Read input file
            input_file = task.input_files[0]
            with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
                contents = f.read()

            # Apply map function
            key_values = self.map_reduce_app.map_func(input_file, contents)

            # Partition output into intermediate files
            intermediate_files: List[List[KeyValue]] = [
                [] for _ in range(task.n_reduce)
            ]

            for kv in key_values:
                # Use hash function to determine which reduce task gets this key
                reduce_id = ihash(kv.key) % task.n_reduce
                intermediate_files[reduce_id].append(kv)

            # Write intermediate files
            for reduce_id in range(task.n_reduce):
                filename = intermediate_filename(task.map_task_id, reduce_id)
                # Use temporary file and atomic rename for crash safety
                temp_file = filename + ".tmp"
                write_key_values_to_file(temp_file, intermediate_files[reduce_id])
                os.rename(temp_file, filename)

            return True

        except Exception as e:
            print(f"Error in map task: {e}")
            return False

    def _execute_reduce_task(self, task: Task) -> bool:
        """Execute a reduce task."""
        try:
            # Read all intermediate files for this reduce task
            all_key_values: List[KeyValue] = []

            for input_file in task.input_files:
                if os.path.exists(input_file):
                    kv_list = read_key_values_from_file(input_file)
                    all_key_values.extend(kv_list)

            if not all_key_values:
                # Create empty output file
                with open(task.output_file, "w") as f:
                    pass
                return True

            # Sort by key
            sorted_kv = sort_key_values(all_key_values)

            # Group by key
            grouped = group_by_key(sorted_kv)

            # Use temporary file and atomic rename for crash safety
            temp_output = task.output_file + ".tmp"

            # Apply reduce function to each group
            with open(temp_output, "w") as f:
                for key, values in grouped:
                    result = self.map_reduce_app.reduce_func(key, values)
                    f.write(f"{key} {result}\n")

            # Atomic rename
            os.rename(temp_output, task.output_file)

            return True

        except Exception as e:
            print(f"Error in reduce task: {e}")
            return False


def load_plugin(plugin_file: str) -> Optional[MapReduceApp]:
    """Load a MapReduce application from a Python file."""
    try:
        # Load the module
        spec = importlib.util.spec_from_file_location("plugin", plugin_file)
        if spec is None or spec.loader is None:
            print(f"Failed to load plugin spec from {plugin_file}")
            return None

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Look for the MapReduce application class
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, MapReduceApp)
                and attr != MapReduceApp
            ):
                return attr()

        print(f"No MapReduceApp subclass found in {plugin_file}")
        return None

    except Exception as e:
        print(f"Error loading plugin {plugin_file}: {e}")
        return None


def main():
    """Main entry point for worker."""
    if len(sys.argv) != 2:
        print("Usage: mr-worker <plugin_file>")
        sys.exit(1)

    plugin_file = sys.argv[1]

    # Load the MapReduce application
    app = load_plugin(plugin_file)
    if app is None:
        print(f"Failed to load MapReduce application from {plugin_file}")
        sys.exit(1)

    # Create and run worker
    worker = Worker(app)
    worker.run()


if __name__ == "__main__":
    main()
