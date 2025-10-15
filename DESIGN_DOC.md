# Distributed MapReduce System - Design Document

## 📋 Overview

This is a Python implementation of the distributed MapReduce framework, inspired by Google's MapReduce paper and MIT 6.5840 coursework. The system processes large datasets by distributing work across multiple worker processes, coordinated by a central coordinator.

## 🎯 System Goals

- **Scalability**: Handle large datasets by distributing work across multiple workers
- **Fault Tolerance**: Automatically detect and recover from worker failures
- **Simplicity**: Easy-to-use interface for implementing custom MapReduce applications
- **Efficiency**: Parallel processing with optimized task scheduling

## 🏗️ Architecture

### Core Components

1. **Coordinator Process**

   - Central orchestrator that manages the entire MapReduce job
   - Maintains task queues and tracks worker progress
   - Handles worker registration, task assignment, and failure detection
   - Runs on a single machine and serves as the control plane

2. **Worker Processes**

   - Execute map and reduce tasks assigned by the coordinator
   - Can be distributed across multiple machines
   - Stateless - each task execution is independent
   - Communicate with coordinator via RPC

3. **RPC Communication Layer**

   - JSON-over-TCP messaging between coordinator and workers
   - Request/response pattern for task assignment and completion
   - Handles network failures and timeouts gracefully
   - **TCP Choice**: Raw TCP over HTTP for lower latency, less overhead, and persistent connections suitable for frequent worker-coordinator communication

4. **MapReduce Applications**
   - User-defined map and reduce functions
   - Pluggable interface allows custom data processing logic
   - Example: Word count, inverted index, etc.

### Data Flow

```
Input Files → Map Phase → Intermediate Files → Reduce Phase → Output Files
     ↓           ↓              ↓               ↓             ↓
   file1.txt  Worker1→mr-0-0   Worker3→combines   mr-out-0
   file2.txt  Worker2→mr-1-1   Worker4→aggregates mr-out-1
```

## 🔄 Execution Flow

### Phase 1: Initialization

1. **Coordinator Startup**

   - Parse command line arguments (nReduce, input files)
   - Create map tasks (one per input file)
   - Create reduce tasks (nReduce tasks)
   - Start RPC server on random port
   - Write coordinator info to file for worker discovery

2. **Worker Startup**
   - Load MapReduce application (e.g., word_count.py)
   - Generate unique worker ID
   - Connect to coordinator via RPC

### Phase 2: Map Phase

1. **Task Assignment**

   - Workers request tasks from coordinator
   - Coordinator assigns map tasks (one input file per task)
   - Each task gets unique ID and timeout tracking

2. **Map Execution**

   - Worker reads input file
   - Applies map function: `map_func(filename, content) → List[KeyValue]`
   - Partitions output by hash(key) % nReduce
   - Writes intermediate files: `mr-{map_task_id}-{reduce_partition}`

3. **Example Map Process**
   ```python
   Input:  "hello world hello python"
   Map:    [("hello","1"), ("world","1"), ("hello","1"), ("python","1")]
   Partition 0: [("hello","1"), ("hello","1")]  → mr-0-0
   Partition 1: [("world","1"), ("python","1")] → mr-0-1
   ```

### Phase 3: Reduce Phase

1. **Phase Transition**

   - Coordinator detects all map tasks completed
   - Switches to reduce phase
   - Only then assigns reduce tasks

2. **Reduce Execution**
   - Worker reads all intermediate files for its partition
   - Sorts and groups by key: `[("hello", ["1","1"]), ("python", ["1"])]`
   - Applies reduce function: `reduce_func(key, values) → aggregated_value`
   - Writes final output: `mr-out-{reduce_partition}`

### Phase 4: Completion

1. All reduce tasks complete
2. Coordinator signals workers to exit
3. Resources cleaned up
4. Job completes successfully

## 🛡️ Fault Tolerance

### Worker Failure Detection

- **Timeout Mechanism**: Tasks timeout after 10 seconds
- **Monitoring Thread**: Background thread checks for timeouts every second
- **Optimized Monitoring**: Only checks active phase (map OR reduce, not both)

### Recovery Strategy

- **Task Reassignment**: Failed tasks reset to IDLE for reassignment
- **Stateless Workers**: New workers can pick up any available task
- **No Data Loss**: Intermediate files persist across worker failures

### Example Failure Scenario

```
1. Worker A assigned Map Task 0
2. Worker A crashes after 5 seconds
3. Coordinator detects timeout at 10 seconds
4. Task 0 reset to IDLE status
5. Worker B requests task → gets Task 0
6. Work continues without data loss
```

## 🔧 Technical Implementation

### Data Structures

```python
@dataclass
class Task:
    task_id: int                    # Unique identifier
    task_type: TaskType            # MAP or REDUCE
    input_files: List[str]         # Files to process
    output_file: str               # Output destination
    n_reduce: int                  # Total reduce partitions

@dataclass
class TaskInfo:
    task: Task                     # Task definition
    status: TaskStatus             # IDLE/IN_PROGRESS/COMPLETED
    worker_id: Optional[str]       # Assigned worker
    start_time: Optional[float]    # For timeout detection
```

### File Naming Convention

- **Input**: `file1.txt`, `file2.txt`
- **Intermediate**: `mr-{map_task_id}-{reduce_partition}`
  - `mr-0-0`: Map task 0 → Reduce partition 0
  - `mr-1-1`: Map task 1 → Reduce partition 1
- **Output**: `mr-out-{reduce_partition}`
  - `mr-out-0`: Final results for partition 0

### RPC Protocol

**Request Task:**

```json
Request:  {"method": "request_task", "params": {"worker_id": "uuid"}}
Response: {"success": true, "result": {"task_id": 0, "task_type": "map", ...}}
```

**Complete Task:**

```json
Request:  {"method": "complete_task", "params": {"worker_id": "uuid", "task_id": 0, "success": true}}
Response: {"success": true, "result": {"acknowledged": true}}
```

## 🚀 Usage Examples

### Basic Word Count

```bash
# Start coordinator
./bin/mr-coordinator 2 input1.txt input2.txt

# Start workers (in separate terminals)
./bin/mr-worker apps/word_count.py
./bin/mr-worker apps/word_count.py
./bin/mr-worker apps/word_count.py
```

### Custom Application

```python
class MyApp:
    def map_func(self, filename: str, content: str) -> List[KeyValue]:
        # Process content, return key-value pairs
        return [KeyValue("key", "value")]

    def reduce_func(self, key: str, values: List[str]) -> str:
        # Aggregate values for key
        return str(len(values))  # Count occurrences
```

## 📁 Project Structure

```
distributed-mapreduce/
├── src/mapreduce/              # Core MapReduce implementation
│   ├── coordinator.py          # Central task coordinator
│   ├── worker.py              # Task execution workers
│   ├── mapreduce.py           # Data structures & utilities
│   └── rpc_utils.py           # RPC communication layer
├── apps/                      # MapReduce applications
│   └── word_count.py          # Word counting example
├── bin/                       # Executable entry points
│   ├── mr-coordinator         # Coordinator launcher
│   └── mr-worker              # Worker launcher
├── tests/                     # Test suite
├── examples/                  # Usage demonstrations
│   └── demo.py                # Complete example
└── docs/                      # Documentation
    ├── NEWCOMER_GUIDE.md      # New developer guide
    ├── QUICK_REFERENCE.md     # API cheat sheet
    └── COMPLETE_LLD_FLOW.md   # Detailed technical flow
```

## 🎯 Key Design Decisions

### 1. Centralized Coordination

- **Choice**: Single coordinator vs distributed consensus
- **Rationale**: Simpler implementation, sufficient for moderate scale
- **Trade-off**: Single point of failure vs complexity

### 2. Phase-Based Execution

- **Choice**: Strict map-then-reduce vs overlapped execution
- **Rationale**: Ensures data consistency, easier fault tolerance
- **Trade-off**: Some latency vs correctness guarantees

### 3. Hash-Based Partitioning

- **Choice**: Hash partitioning vs range partitioning
- **Rationale**: Better load distribution, no need for key ordering
- **Trade-off**: Less locality vs balanced workload

### 4. File-Based Communication

- **Choice**: Files vs in-memory data structures
- **Rationale**: Persistence across failures, language independence
- **Trade-off**: I/O overhead vs fault tolerance

## 🔍 Performance Characteristics

### Scalability

- **Map Phase**: Linear scaling with number of input files
- **Reduce Phase**: Limited by number of reduce partitions
- **Network**: Minimal coordinator-worker communication
- **Storage**: O(input_size) intermediate file space

### Fault Tolerance

- **Detection Time**: 10 seconds (configurable timeout)
- **Recovery Time**: Immediate task reassignment
- **Data Loss**: None (files persist across failures)
- **Availability**: Continues with fewer workers

## 🚧 Limitations & Future Improvements

### Current Limitations

1. **Single Point of Failure**: Coordinator failure stops entire job
2. **No Dynamic Scaling**: Worker count fixed at startup
3. **Local File System**: No distributed storage integration
4. **Basic Load Balancing**: No consideration of worker capacity

### Potential Improvements

1. **Coordinator High Availability**: Master election, state replication
2. **Dynamic Worker Management**: Auto-scaling based on workload
3. **Distributed Storage**: HDFS, S3 integration
4. **Advanced Scheduling**: Locality-aware task assignment
5. **Streaming Support**: Real-time data processing
6. **Resource Management**: CPU, memory limits per task

## 🧪 Testing Strategy

### Unit Tests

- Individual component testing (coordinator, worker, RPC)
- Mock-based isolation of dependencies
- Edge case coverage (empty files, failures)

### Integration Tests

- End-to-end MapReduce job execution
- Multi-worker coordination scenarios
- Fault injection and recovery testing

### Performance Tests

- Large dataset processing benchmarks
- Scalability testing with multiple workers
- Failure recovery time measurements

## 📊 Monitoring & Observability

### Built-in Logging

- Task assignment and completion events
- Worker registration and failure detection
- Phase transition notifications
- Performance timing information

### Metrics Available

- Task execution times
- Worker utilization
- Failure rates and recovery times
- Data throughput measurements

## 🔐 Security Considerations

### Current Security Model

- **Trust-based**: Workers trusted to execute tasks correctly
- **No Authentication**: Open coordinator access
- **Local Network**: Assumes secure network environment

### Production Security Needs

- Worker authentication and authorization
- Encrypted RPC communication
- Input/output data encryption
- Resource usage limits and sandboxing

---

## 📚 Getting Started

### Quick Start

```bash
# Run the demo
python examples/demo.py

# Or manual execution
./bin/mr-coordinator 2 input1.txt input2.txt
./bin/mr-worker apps/word_count.py
```

### Development Setup

```bash
# Install dependencies
pip install -e .

# Run tests
python -m pytest tests/

# Create custom application
# See apps/word_count.py for template
```

This design document serves as the authoritative reference for understanding, developing, and extending the distributed MapReduce system.
