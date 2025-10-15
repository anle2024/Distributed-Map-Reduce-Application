"""
Basic tests for distributed MapReduce system.
Tests basic functionality and correctness.
"""

import unittest
import os
import sys
import tempfile
import shutil
import time
import subprocess
import threading
from typing import List

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mapreduce.coordinator import make_coordinator
from mapreduce.worker import Worker, load_plugin


class TestMapReduce(unittest.TestCase):
    """Test suite for MapReduce system."""

    def setUp(self):
        """Set up test environment."""
        # Create temporary directory for test files
        self.test_dir = tempfile.mkdtemp()
        self.original_dir = os.getcwd()
        os.chdir(self.test_dir)

        # Create test input files
        self.input_files = []
        self._create_test_files()

    def tearDown(self):
        """Clean up test environment and MapReduce files."""
        # Clean up MapReduce output files in original directory
        os.chdir(self.original_dir)
        import glob

        for pattern in ["mr-out-*", "coordinator_info.txt", "mr-*-*"]:
            for file in glob.glob(pattern):
                try:
                    os.remove(file)
                except OSError:
                    pass

        # Remove temp directory
        shutil.rmtree(self.test_dir)

    def _create_test_files(self):
        """Create test input files."""
        # File 1: Simple text
        with open("test1.txt", "w") as f:
            f.write("hello world\nhello python\nworld of programming")
        self.input_files.append("test1.txt")

        # File 2: More text
        with open("test2.txt", "w") as f:
            f.write("python programming\nhello again\nworld wide web")
        self.input_files.append("test2.txt")

        # File 3: Edge cases
        with open("test3.txt", "w") as f:
            f.write("a b c\na a a\nb b\nc")
        self.input_files.append("test3.txt")

    def test_word_count_sequential(self):
        """Test word count with sequential execution."""
        # Copy necessary files to test directory
        import sys
        import shutil

        base_dir = os.path.dirname(os.path.dirname(__file__))

        # Copy files from src/mapreduce to test directory
        src_dir = os.path.join(base_dir, "src", "mapreduce")
        for file in [
            "mapreduce.py",
            "rpc_utils.py",
            "coordinator.py",
            "worker.py",
            "__init__.py",
        ]:
            shutil.copy(os.path.join(src_dir, file), self.test_dir)

        # Copy apps directory (instead of mrapps)
        apps_src = os.path.join(base_dir, "apps")
        apps_dst = os.path.join(self.test_dir, "apps")
        shutil.copytree(apps_src, apps_dst)

        # Add current directory to Python path for imports
        sys.path.insert(0, self.test_dir)
        sys.path.insert(0, os.path.join(self.test_dir, "apps"))

        try:
            # Load word count application
            from word_count import WordCountApp

            app = WordCountApp()

            # Create coordinator
            n_reduce = 3
            coordinator = make_coordinator(self.input_files, n_reduce)

            # Create and run worker
            worker = Worker(app)

            # Run worker in thread
            worker_thread = threading.Thread(target=worker.run)
            worker_thread.start()

            # Wait for completion
            timeout = 30
            start_time = time.time()
            while not coordinator.done() and time.time() - start_time < timeout:
                time.sleep(0.1)

            worker_thread.join(timeout=5)
            coordinator.stop()

            # Check output files exist
            for i in range(n_reduce):
                output_file = f"mr-out-{i}"
                self.assertTrue(
                    os.path.exists(output_file), f"Output file {output_file} not found"
                )

            # Read and combine all output
            word_counts = {}
            for i in range(n_reduce):
                output_file = f"mr-out-{i}"
                if os.path.exists(output_file):
                    with open(output_file, "r") as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                parts = line.split(" ", 1)
                                if len(parts) == 2:
                                    word, count = parts
                                    word_counts[word] = int(count)

            # Check expected word counts
            expected_words = {
                "hello": 3,
                "world": 3,
                "python": 2,
                "programming": 2,
                "a": 4,
                "b": 3,
                "c": 2,
            }

            for word, expected_count in expected_words.items():
                self.assertIn(word, word_counts, f"Word '{word}' not found in output")
                self.assertEqual(
                    word_counts[word],
                    expected_count,
                    f"Expected {expected_count} occurrences of '{word}', got {word_counts[word]}",
                )

        finally:
            sys.path.remove(self.test_dir)

    def test_empty_file_handling(self):
        """Test handling of empty input files."""
        # Create empty file
        with open("empty.txt", "w") as f:
            pass

        import sys

        base_dir = os.path.dirname(os.path.dirname(__file__))
        sys.path.insert(0, base_dir)
        sys.path.insert(0, os.path.join(base_dir, "apps"))

        try:
            from word_count import WordCountApp

            app = WordCountApp()

            # Test map function with empty content
            result = app.map_func("empty.txt", "")
            self.assertEqual(
                len(result), 0, "Empty file should produce no key-value pairs"
            )

            # Test reduce function with empty values
            result = app.reduce_func("word", [])
            self.assertEqual(result, "0", "Reduce with empty values should return 0")

        finally:
            sys.path.remove(base_dir)


class TestMapReduceApp(unittest.TestCase):
    """Test MapReduce application functions directly."""

    def setUp(self):
        """Set up test app."""
        import sys

        base_dir = os.path.dirname(os.path.dirname(__file__))
        sys.path.insert(0, base_dir)
        sys.path.insert(0, os.path.join(base_dir, "apps"))

        from word_count import WordCountApp

        self.app = WordCountApp()

    def tearDown(self):
        """Clean up."""
        import sys

        base_dir = os.path.dirname(os.path.dirname(__file__))
        if base_dir in sys.path:
            sys.path.remove(base_dir)

    def test_map_function(self):
        """Test map function."""
        content = "Hello World\nHello Python!\nWorld of code."
        result = self.app.map_func("test.txt", content)

        # Count expected words
        expected_words = ["hello", "world", "hello", "python", "world", "of", "code"]
        self.assertEqual(len(result), len(expected_words))

        # Check all words are present
        result_words = [kv.key for kv in result]
        for word in expected_words:
            self.assertIn(word, result_words)

    def test_reduce_function(self):
        """Test reduce function."""
        # Test reducing word counts
        result = self.app.reduce_func("hello", ["1", "1", "1"])
        self.assertEqual(result, "3")

        result = self.app.reduce_func("world", ["1"])
        self.assertEqual(result, "1")

        result = self.app.reduce_func("empty", [])
        self.assertEqual(result, "0")


if __name__ == "__main__":
    unittest.main()
