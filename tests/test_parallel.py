"""
Tests for parallel execution and crash recovery in MapReduce system.
"""

import unittest
import os
import tempfile
import shutil
import time
import threading
import signal
import subprocess
import sys
from typing import List

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mapreduce.coordinator import make_coordinator
from mapreduce.worker import Worker


class TestParallelExecution(unittest.TestCase):
    """Test parallel execution capabilities."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.original_dir = os.getcwd()
        os.chdir(self.test_dir)

        # Create multiple test files for parallel processing
        self.input_files = []
        for i in range(5):
            filename = f"input_{i}.txt"
            with open(filename, "w") as f:
                # Write different content in each file
                f.write(f"file{i} " * 100)  # 100 occurrences of "file{i}"
                f.write("common word " * 50)  # 50 occurrences of "common" and "word"
            self.input_files.append(filename)

        # Copy necessary files from src/mapreduce
        base_dir = os.path.dirname(os.path.dirname(__file__))
        src_dir = os.path.join(base_dir, "src", "mapreduce")
        for file in ["mapreduce.py", "rpc_utils.py", "coordinator.py", "worker.py"]:
            shutil.copy(os.path.join(src_dir, file), self.test_dir)

        # Copy apps directory
        apps_src = os.path.join(base_dir, "apps")
        apps_dst = os.path.join(self.test_dir, "apps")
        shutil.copytree(apps_src, apps_dst)

    def tearDown(self):
        """Clean up test environment and MapReduce files."""
        # Clean up MapReduce output files in current directory
        import glob

        for pattern in ["mr-out-*", "coordinator_info.txt", "mr-*-*"]:
            for file in glob.glob(pattern):
                try:
                    os.remove(file)
                except OSError:
                    pass

        # Change back to original directory and remove temp directory
        os.chdir(self.original_dir)
        shutil.rmtree(self.test_dir)

    def test_multiple_workers(self):
        """Test running multiple workers in parallel."""
        sys.path.insert(0, self.test_dir)
        sys.path.insert(0, os.path.join(self.test_dir, "apps"))

        try:
            from word_count import WordCountApp

            # Create coordinator
            n_reduce = 2
            coordinator = make_coordinator(self.input_files, n_reduce)

            # Create multiple workers
            workers = []
            worker_threads = []

            for i in range(3):  # 3 workers
                worker = Worker(WordCountApp())
                workers.append(worker)

                thread = threading.Thread(target=worker.run)
                worker_threads.append(thread)
                thread.start()

            # Wait for completion
            timeout = 60
            start_time = time.time()
            while not coordinator.done() and time.time() - start_time < timeout:
                time.sleep(0.1)

            # Wait for workers to finish
            for thread in worker_threads:
                thread.join(timeout=10)

            coordinator.stop()

            # Verify output
            self.assertTrue(coordinator.done(), "Job should be completed")

            # Check output files
            for i in range(n_reduce):
                output_file = f"mr-out-{i}"
                self.assertTrue(
                    os.path.exists(output_file),
                    f"Output file {output_file} should exist",
                )

            # Verify word counts
            word_counts = {}
            for i in range(n_reduce):
                output_file = f"mr-out-{i}"
                with open(output_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            parts = line.split(" ", 1)
                            if len(parts) == 2:
                                word, count = parts
                                word_counts[word] = word_counts.get(word, 0) + int(
                                    count
                                )

            # Check expected counts
            self.assertEqual(word_counts.get("common", 0), 250)  # 50 * 5 files
            self.assertEqual(word_counts.get("word", 0), 250)  # 50 * 5 files

            # Check individual file words
            for i in range(5):
                file_word = f"file{i}"
                self.assertEqual(word_counts.get(file_word, 0), 100)  # 100 * 1 file

        finally:
            sys.path.remove(self.test_dir)


class CrashingWorker(Worker):
    """Worker that crashes after processing a certain number of tasks."""

    def __init__(self, map_reduce_app, crash_after=2):
        super().__init__(map_reduce_app)
        self.tasks_completed = 0
        self.crash_after = crash_after

    def _report_completion(self, task_id: int, success: bool, error_message: str = ""):
        """Override to simulate crash."""
        super()._report_completion(task_id, success, error_message)

        if success:
            self.tasks_completed += 1
            if self.tasks_completed >= self.crash_after:
                print(
                    f"Worker {self.worker_id} simulating crash after {self.tasks_completed} tasks"
                )
                # Simulate crash by exiting
                raise SystemExit("Simulated crash")


class TestCrashRecovery(unittest.TestCase):
    """Test crash recovery capabilities."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.original_dir = os.getcwd()
        os.chdir(self.test_dir)

        # Create test files
        self.input_files = []
        for i in range(3):
            filename = f"crash_test_{i}.txt"
            with open(filename, "w") as f:
                f.write(f"test{i} " * 50)
                f.write("crash recovery test " * 30)
            self.input_files.append(filename)

        # Copy necessary files from src/mapreduce
        base_dir = os.path.dirname(os.path.dirname(__file__))
        src_dir = os.path.join(base_dir, "src", "mapreduce")
        for file in ["mapreduce.py", "rpc_utils.py", "coordinator.py", "worker.py"]:
            shutil.copy(os.path.join(src_dir, file), self.test_dir)

        # Copy apps directory
        apps_src = os.path.join(base_dir, "apps")
        apps_dst = os.path.join(self.test_dir, "apps")
        shutil.copytree(apps_src, apps_dst)

    def tearDown(self):
        """Clean up test environment and MapReduce files."""
        # Clean up MapReduce output files in current directory
        import glob

        for pattern in ["mr-out-*", "coordinator_info.txt", "mr-*-*"]:
            for file in glob.glob(pattern):
                try:
                    os.remove(file)
                except OSError:
                    pass

        # Change back to original directory and remove temp directory
        os.chdir(self.original_dir)
        shutil.rmtree(self.test_dir)

    def test_worker_crash_recovery(self):
        """Test that coordinator handles worker crashes and reassigns tasks."""
        sys.path.insert(0, self.test_dir)
        sys.path.insert(0, os.path.join(self.test_dir, "apps"))

        try:
            from word_count import WordCountApp

            # Create coordinator
            n_reduce = 2
            coordinator = make_coordinator(self.input_files, n_reduce)

            # Create normal workers and crashing workers
            workers = []
            worker_threads = []

            # Start with a crashing worker
            crashing_worker = CrashingWorker(WordCountApp(), crash_after=1)
            workers.append(crashing_worker)

            def run_crashing_worker():
                try:
                    crashing_worker.run()
                except SystemExit:
                    print("Crashing worker exited as expected")

            crash_thread = threading.Thread(target=run_crashing_worker)
            worker_threads.append(crash_thread)
            crash_thread.start()

            # Wait a bit for the crash to happen
            time.sleep(2)

            # Start backup workers to complete the job
            for i in range(2):
                worker = Worker(WordCountApp())
                workers.append(worker)

                thread = threading.Thread(target=worker.run)
                worker_threads.append(thread)
                thread.start()

            # Wait for completion
            timeout = 60
            start_time = time.time()
            while not coordinator.done() and time.time() - start_time < timeout:
                time.sleep(0.1)

            # Wait for workers to finish
            for thread in worker_threads:
                thread.join(timeout=10)

            coordinator.stop()

            # Verify the job completed despite the crash
            self.assertTrue(
                coordinator.done(), "Job should complete despite worker crash"
            )

            # Verify output correctness
            word_counts = {}
            for i in range(n_reduce):
                output_file = f"mr-out-{i}"
                self.assertTrue(os.path.exists(output_file))

                with open(output_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            parts = line.split(" ", 1)
                            if len(parts) == 2:
                                word, count = parts
                                word_counts[word] = word_counts.get(word, 0) + int(
                                    count
                                )

            # Check expected word counts
            self.assertEqual(word_counts.get("crash", 0), 90)  # 30 * 3 files
            self.assertEqual(word_counts.get("recovery", 0), 90)  # 30 * 3 files
            self.assertEqual(word_counts.get("test", 0), 90)  # 30 * 3 files

        finally:
            sys.path.remove(self.test_dir)


if __name__ == "__main__":
    unittest.main()
