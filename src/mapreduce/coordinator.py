"""
Coordinator implementation for distributed MapReduce.
Manages task distribution, worker coordination, and failure recovery.
"""

import threading
import time
import os
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field

from .mapreduce import (
    Task,
    TaskType,
    TaskStatus,
    TaskRequest,
    TaskResponse,
    TaskCompletion,
)
from .rpc_utils import RPCServer


@dataclass
class TaskInfo:
    """Information about a task's execution."""

    task: Task
    status: TaskStatus = TaskStatus.IDLE
    worker_id: Optional[str] = None
    start_time: Optional[float] = None
    completion_time: Optional[float] = None


class Coordinator:
    """
    Coordinator for distributed MapReduce jobs.
    Manages task distribution, tracks worker progress, and handles failures.
    """

    def __init__(self, files: List[str], n_reduce: int):
        """
        Initialize the coordinator.

        Args:
            files: Input files for map tasks
            n_reduce: Number of reduce tasks
        """
        self.files = files
        self.n_reduce = n_reduce
        self.lock = threading.Lock()

        # Task management
        self.map_tasks: Dict[int, TaskInfo] = {}
        self.reduce_tasks: Dict[int, TaskInfo] = {}
        self.next_task_id = 0

        # Worker tracking
        self.active_workers: Set[str] = set()

        # Job state
        self.map_phase_complete = False
        self.all_tasks_complete = False

        # RPC server
        self.rpc_server = RPCServer()
        self.coordinator_port: Optional[int] = None

        # Initialize tasks
        self._create_map_tasks()
        self._create_reduce_tasks()

        # Start background thread for monitoring
        self.monitor_thread = threading.Thread(target=self._monitor_tasks, daemon=True)
        self.monitor_thread.start()

    def start(self) -> int:
        """Start the coordinator RPC server."""
        self.rpc_server.register_handler("request_task", self._handle_task_request)
        self.rpc_server.register_handler("complete_task", self._handle_task_completion)

        self.coordinator_port = self.rpc_server.start()
        print(f"Coordinator started on port {self.coordinator_port}")

        # Write coordinator info to file for workers to find
        with open("coordinator_info.txt", "w") as f:
            f.write(f"localhost:{self.coordinator_port}\n")

        return self.coordinator_port

    def stop(self):
        """Stop the coordinator."""
        self.rpc_server.stop()

        # Clean up coordinator info file
        try:
            os.remove("coordinator_info.txt")
        except OSError:
            pass

    def done(self) -> bool:
        """Check if the MapReduce job is complete."""
        with self.lock:
            return self.all_tasks_complete

    def _create_map_tasks(self):
        """Create map tasks for input files."""
        for i, filename in enumerate(self.files):
            task = Task(
                task_id=self.next_task_id,
                task_type=TaskType.MAP,
                input_files=[filename],
                output_file="",  # Map tasks write to multiple intermediate files
                n_reduce=self.n_reduce,
                map_task_id=i,
            )
            self.map_tasks[self.next_task_id] = TaskInfo(task=task)
            self.next_task_id += 1

    def _create_reduce_tasks(self):
        """Create reduce tasks."""
        for i in range(self.n_reduce):
            # Reduce tasks will read from all map outputs
            input_files = []
            for map_id in range(len(self.files)):
                input_files.append(f"mr-{map_id}-{i}")

            task = Task(
                task_id=self.next_task_id,
                task_type=TaskType.REDUCE,
                input_files=input_files,
                output_file=f"mr-out-{i}",
                n_reduce=self.n_reduce,
                reduce_task_id=i,
            )
            self.reduce_tasks[self.next_task_id] = TaskInfo(task=task)
            self.next_task_id += 1

    def _handle_task_request(self, worker_id: str) -> Dict:
        """Handle task request from worker."""
        with self.lock:
            self.active_workers.add(worker_id)

            # Check if we should assign a task
            task = self._get_available_task()

            if task is None:
                # No task available - either waiting or done
                if self.all_tasks_complete:
                    return Task(
                        task_id=-1,
                        task_type=TaskType.EXIT,
                        input_files=[],
                        output_file="",
                        n_reduce=self.n_reduce,
                    ).to_dict()
                else:
                    return Task(
                        task_id=-1,
                        task_type=TaskType.WAIT,
                        input_files=[],
                        output_file="",
                        n_reduce=self.n_reduce,
                    ).to_dict()

            # Assign task to worker
            if task.task_type == TaskType.MAP:
                task_info = self.map_tasks[task.task_id]
            else:
                task_info = self.reduce_tasks[task.task_id]

            task_info.status = TaskStatus.IN_PROGRESS
            task_info.worker_id = worker_id
            task_info.start_time = time.time()

            print(
                f"Assigned {task.task_type.value} task {task.task_id} to worker {worker_id}"
            )
            return task.to_dict()

    def _handle_task_completion(
        self, worker_id: str, task_id: int, success: bool, error_message: str = ""
    ) -> Dict:
        """Handle task completion notification from worker."""
        with self.lock:
            print(f"Worker {worker_id} completed task {task_id}, success: {success}")

            # Find the task
            task_info = None
            if task_id in self.map_tasks:
                task_info = self.map_tasks[task_id]
            elif task_id in self.reduce_tasks:
                task_info = self.reduce_tasks[task_id]

            if task_info is None:
                return {"acknowledged": False, "error": "Task not found"}

            if success:
                task_info.status = TaskStatus.COMPLETED
                task_info.completion_time = time.time()

                # Check if we can advance phases
                self._check_phase_completion()
            else:
                print(f"Task {task_id} failed: {error_message}")
                # Reset task to be reassigned
                task_info.status = TaskStatus.IDLE
                task_info.worker_id = None
                task_info.start_time = None

            return {"acknowledged": True}

    def _get_available_task(self) -> Optional[Task]:
        """Get the next available task for assignment."""
        # During map phase, only assign map tasks
        if not self.map_phase_complete:
            for task_info in self.map_tasks.values():
                if task_info.status == TaskStatus.IDLE:
                    return task_info.task
            return None

        # During reduce phase, assign reduce tasks
        for task_info in self.reduce_tasks.values():
            if task_info.status == TaskStatus.IDLE:
                return task_info.task

        return None

    def _check_phase_completion(self):
        """Check if current phase is complete and update job state."""
        # Check if map phase is complete
        if not self.map_phase_complete:
            all_maps_done = all(
                task_info.status == TaskStatus.COMPLETED
                for task_info in self.map_tasks.values()
            )
            if all_maps_done:
                self.map_phase_complete = True
                print("Map phase completed, starting reduce phase")

        # Check if all tasks are complete
        if self.map_phase_complete:
            all_reduces_done = all(
                task_info.status == TaskStatus.COMPLETED
                for task_info in self.reduce_tasks.values()
            )
            if all_reduces_done:
                self.all_tasks_complete = True
                print("All tasks completed")

    def _monitor_tasks(self):
        """Background thread to monitor task timeouts and reassign failed tasks."""
        while True:
            time.sleep(1)  # Check every second

            with self.lock:
                if self.all_tasks_complete:
                    break

                current_time = time.time()

                if not self.map_phase_complete:
                    # During map phase, only check map tasks
                    for task_info in self.map_tasks.values():
                        if (
                            task_info.status == TaskStatus.IN_PROGRESS
                            and task_info.start_time is not None
                            and current_time - task_info.start_time > 10
                        ):  # 10 second timeout

                            print(
                                f"Map task {task_info.task.task_id} timed out, reassigning"
                            )
                            task_info.status = TaskStatus.IDLE
                            task_info.worker_id = None
                            task_info.start_time = None
                else:
                    # During reduce phase, only check reduce tasks
                    for task_info in self.reduce_tasks.values():
                        if (
                            task_info.status == TaskStatus.IN_PROGRESS
                            and task_info.start_time is not None
                            and current_time - task_info.start_time > 10
                        ):  # 10 second timeout

                            print(
                                f"Reduce task {task_info.task.task_id} timed out, reassigning"
                            )
                            task_info.status = TaskStatus.IDLE
                            task_info.worker_id = None
                            task_info.start_time = None


def make_coordinator(files: List[str], n_reduce: int) -> Coordinator:
    """Create and start a coordinator."""
    coordinator = Coordinator(files, n_reduce)
    coordinator.start()
    return coordinator


def main():
    """Main entry point for coordinator."""
    import sys

    if len(sys.argv) < 3:
        print("Usage: mr-coordinator <nReduce> <input_files...>")
        sys.exit(1)

    n_reduce = int(sys.argv[1])
    input_files = sys.argv[2:]

    # Verify input files exist
    for filename in input_files:
        if not os.path.exists(filename):
            print(f"Input file not found: {filename}")
            sys.exit(1)

    coordinator = make_coordinator(input_files, n_reduce)

    try:
        # Wait for completion
        while not coordinator.done():
            time.sleep(1)

        print("MapReduce job completed successfully")

    except KeyboardInterrupt:
        print("\nShutting down coordinator...")
    finally:
        coordinator.stop()


if __name__ == "__main__":
    main()
