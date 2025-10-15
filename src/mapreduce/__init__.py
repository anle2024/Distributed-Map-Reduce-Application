"""
MapReduce package initialization.
"""

from .mapreduce import (
    TaskType,
    TaskStatus,
    Task,
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

from .coordinator import Coordinator, make_coordinator
from .worker import Worker, load_plugin
from .rpc_utils import RPCServer, RPCClient

__version__ = "1.0.0"
__author__ = "MapReduce Team"
__email__ = "team@mapreduce.com"

__all__ = [
    # Core classes
    "TaskType",
    "TaskStatus",
    "Task",
    "KeyValue",
    "MapReduceApp",
    # Main components
    "Coordinator",
    "Worker",
    "RPCServer",
    "RPCClient",
    # Factory functions
    "make_coordinator",
    "load_plugin",
    # Utility functions
    "ihash",
    "intermediate_filename",
    "output_filename",
    "read_key_values_from_file",
    "write_key_values_to_file",
    "write_final_output",
    "sort_key_values",
    "group_by_key",
]
