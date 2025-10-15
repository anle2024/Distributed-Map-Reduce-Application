#!/usr/bin/env python3
"""
Demonstration script for the distributed MapReduce system.
Creates sample input files and runs a complete MapReduce job.
"""

import os
import sys
import time
import threading
import tempfile
import subprocess

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def cleanup_files(input_files=None):
    """Clean up all MapReduce-related files."""
    import glob

    # Clean up input files if provided
    if input_files:
        for filename in input_files:
            try:
                os.remove(filename)
                print(f"Removed {filename}")
            except OSError:
                pass

    # Clean up MapReduce output and intermediate files
    patterns = ["mr-out-*", "mr-*-*", "coordinator_info.txt", "sample_*.txt"]
    for pattern in patterns:
        for filename in glob.glob(pattern):
            try:
                os.remove(filename)
                print(f"Removed {filename}")
            except OSError:
                pass


def create_sample_files():
    """Create sample input files for testing."""
    # Sample text content for demonstration
    sample_texts = [
        """
        The quick brown fox jumps over the lazy dog.
        This is a sample text file for MapReduce testing.
        MapReduce is a programming model for processing large datasets.
        """,
        """
        Python is a powerful programming language.
        MapReduce helps process big data efficiently.
        The distributed system coordinates multiple workers.
        """,
        """
        Hello world from MapReduce!
        This system demonstrates distributed computing.
        Workers execute map and reduce tasks in parallel.
        """,
    ]

    filenames = []
    for i, text in enumerate(sample_texts):
        filename = f"sample_{i+1}.txt"
        with open(filename, "w") as f:
            f.write(text.strip())
        filenames.append(filename)
        print(f"Created {filename}")

    return filenames


def run_demo():
    """Run a complete MapReduce demonstration."""
    print("=== Distributed MapReduce Demo ===\n")

    # Create sample input files
    print("1. Creating sample input files...")
    input_files = create_sample_files()
    print(f"Created {len(input_files)} input files\n")

    try:
        from mapreduce.coordinator import make_coordinator
        from mapreduce.worker import Worker

        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "apps"))
        from word_count import WordCountApp

        # Start coordinator
        print("2. Starting coordinator...")
        n_reduce = 2
        coordinator = make_coordinator(input_files, n_reduce)
        print(f"Coordinator started with {n_reduce} reduce tasks\n")

        # Start workers in separate threads
        print("3. Starting workers...")
        workers = []
        worker_threads = []

        for i in range(3):  # Start 3 workers
            worker = Worker(WordCountApp())
            workers.append(worker)

            thread = threading.Thread(target=worker.run, name=f"Worker-{i+1}")
            worker_threads.append(thread)
            thread.start()
            print(f"Started worker {i+1}")

        print(f"Started {len(workers)} workers\n")

        # Monitor progress
        print("4. Processing...")
        start_time = time.time()

        while not coordinator.done():
            time.sleep(0.5)
            elapsed = time.time() - start_time
            print(f"   Processing... ({elapsed:.1f}s elapsed)")

        processing_time = time.time() - start_time
        print(f"   Completed in {processing_time:.1f} seconds\n")

        # Wait for workers to finish
        print("5. Finishing workers...")
        for thread in worker_threads:
            thread.join(timeout=5)

        coordinator.stop()

        # Display results
        print("6. Results:")
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
                                word_counts[word] = word_counts.get(word, 0) + int(
                                    count
                                )

        # Sort and display top words
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)

        print("\nTop words found:")
        print("-" * 30)
        for word, count in sorted_words[:15]:  # Show top 15 words
            print(f"{word:15} {count:3}")

        print(f"\nTotal unique words: {len(word_counts)}")
        print(f"Total word occurrences: {sum(word_counts.values())}")

        # Cleanup
        print("\n7. Cleaning up...")
        cleanup_files(input_files)

        print("Demo completed successfully!")

    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure all required files are in the current directory")
    except Exception as e:
        print(f"Error during demo: {e}")
    finally:
        # Ensure coordinator is stopped
        if "coordinator" in locals():
            coordinator.stop()

        # Ensure cleanup happens even if there was an error
        if "input_files" in locals():
            print("Performing emergency cleanup...")
            cleanup_files(input_files)
        else:
            print("Performing emergency cleanup...")
            cleanup_files()


if __name__ == "__main__":
    run_demo()
