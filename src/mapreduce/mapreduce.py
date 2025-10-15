"""
Core MapReduce data structures and utilities for distributed MapReduce implementation.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Any, Callable, Tuple, Dict
import json
import hashlib


class TaskType(Enum):
    """Types of tasks in MapReduce."""

    MAP = "map"
    REDUCE = "reduce"
    WAIT = "wait"
    EXIT = "exit"


class TaskStatus(Enum):
    """Status of tasks."""

    IDLE = "idle"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


@dataclass
class KeyValue:
    """Key-value pair for MapReduce operations."""

    key: str
    value: str

    def to_dict(self) -> Dict[str, str]:
        """Convert KeyValue object to dictionary for JSON serialization."""
        return {"key": self.key, "value": self.value}

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "KeyValue":
        """Create KeyValue object from dictionary (from JSON deserialization)."""
        return cls(key=data["key"], value=data["value"])


@dataclass
class Task:
    """Represents a MapReduce task."""

    task_id: int
    task_type: TaskType
    input_files: List[str]
    output_file: str
    n_reduce: int
    map_task_id: int = -1  # Used by REDUCE tasks: which map task's output to read
    reduce_task_id: int = -1  # Used by MAP tasks: which reduce partition to write to

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "task_id": self.task_id,
            "task_type": self.task_type.value,
            "input_files": self.input_files,
            "output_file": self.output_file,
            "n_reduce": self.n_reduce,
            "map_task_id": self.map_task_id,
            "reduce_task_id": self.reduce_task_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """Create from dictionary for JSON deserialization."""
        return cls(
            task_id=data["task_id"],
            task_type=TaskType(data["task_type"]),
            input_files=data["input_files"],
            output_file=data["output_file"],
            n_reduce=data["n_reduce"],
            map_task_id=data.get("map_task_id", -1),
            reduce_task_id=data.get("reduce_task_id", -1),
        )


@dataclass
class TaskRequest:
    """Request for a task from worker to coordinator."""

    worker_id: str

    def to_dict(self) -> Dict[str, str]:
        return {"worker_id": self.worker_id}


@dataclass
class TaskResponse:
    """Response with task assignment from coordinator to worker."""

    task: Task

    def to_dict(self) -> Dict[str, Any]:
        return {"task": self.task.to_dict()}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskResponse":
        return cls(task=Task.from_dict(data["task"]))


@dataclass
class TaskCompletion:
    """Notification of task completion from worker to coordinator."""

    worker_id: str
    task_id: int
    success: bool
    error_message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "task_id": self.task_id,
            "success": self.success,
            "error_message": self.error_message,
        }


class MapReduceApp:
    """Base class for MapReduce applications."""

    def map_func(self, filename: str, contents: str) -> List[KeyValue]:
        """
        Map function to be implemented by applications.

        Args:
            filename: Input filename
            contents: File contents

        Returns:
            List of key-value pairs
        """
        raise NotImplementedError

    def reduce_func(self, key: str, values: List[str]) -> str:
        """
        Reduce function to be implemented by applications.

        Args:
            key: The key to reduce
            values: List of values for this key

        Returns:
            Reduced value as string
        """
        raise NotImplementedError


def ihash(key: str) -> int:
    """
    Hash function for distributing keys to reduce tasks.
    Uses Python's built-in hash function modulo 2^31-1 for consistency.
    """
    return hash(key) % (2**31 - 1)


def intermediate_filename(map_task_id: int, reduce_task_id: int) -> str:
    """Generate intermediate filename for map output."""
    return f"mr-{map_task_id}-{reduce_task_id}"


def output_filename(reduce_task_id: int) -> str:
    """Generate final output filename for reduce output."""
    return f"mr-out-{reduce_task_id}"


def read_key_values_from_file(filename: str) -> List[KeyValue]:
    """Read key-value pairs from a JSON file."""
    try:
        with open(filename, "r") as f:
            kv_list = []
            for line in f:
                line = line.strip()
                if line:
                    data = json.loads(line)
                    kv_list.append(KeyValue.from_dict(data))
            return kv_list
    except FileNotFoundError:
        return []
    except Exception as e:
        print(f"Error reading file {filename}: {e}")
        return []


def write_key_values_to_file(filename: str, key_values: List[KeyValue]):
    """Write key-value pairs to a JSON file."""
    try:
        with open(filename, "w") as f:
            for kv in key_values:
                json.dump(kv.to_dict(), f)
                f.write("\n")
    except Exception as e:
        print(f"Error writing file {filename}: {e}")
        raise


def write_final_output(filename: str, key: str, value: str):
    """Write final reduce output in the correct format."""
    try:
        with open(filename, "a") as f:
            f.write(f"{key} {value}\n")
    except Exception as e:
        print(f"Error writing output file {filename}: {e}")
        raise


def sort_key_values(key_values: List[KeyValue]) -> List[KeyValue]:
    """Sort key-value pairs by key."""
    return sorted(key_values, key=lambda kv: kv.key)


def group_by_key(sorted_key_values: List[KeyValue]) -> List[Tuple[str, List[str]]]:
    """Group sorted key-value pairs by key."""
    if not sorted_key_values:
        return []

    groups = []
    current_key = sorted_key_values[0].key
    current_values = [sorted_key_values[0].value]

    for kv in sorted_key_values[1:]:
        if kv.key == current_key:
            current_values.append(kv.value)
        else:
            groups.append((current_key, current_values))
            current_key = kv.key
            current_values = [kv.value]

    groups.append((current_key, current_values))
    return groups
