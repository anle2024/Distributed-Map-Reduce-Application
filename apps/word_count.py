"""
Word Count MapReduce application.
Counts the occurrences of each word in the input files.
"""

import re
import sys
import os
from typing import List

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mapreduce import MapReduceApp, KeyValue


class WordCountApp(MapReduceApp):
    """Word count MapReduce application."""

    def map_func(self, filename: str, contents: str) -> List[KeyValue]:
        """
        Map function for word count.

        Args:
            filename: Input filename (not used in word count)
            contents: File contents

        Returns:
            List of (word, "1") key-value pairs
        """
        # Split into words using regex to handle punctuation
        words = re.findall(r"\b[a-zA-Z0-9]+\b", contents)

        # Convert to lowercase and create key-value pairs
        key_values = []
        for word in words:
            word_lower = word.lower()
            key_values.append(KeyValue(key=word_lower, value="1"))

        return key_values

    def reduce_func(self, key: str, values: List[str]) -> str:
        """
        Reduce function for word count.

        Args:
            key: The word
            values: List of counts (all "1"s)

        Returns:
            Total count as string
        """
        # Sum all the counts
        total_count = len(values)  # Each value is "1"
        return str(total_count)


# This is needed for the plugin loading mechanism
def get_mapreduce_app():
    """Factory function to create the MapReduce application."""
    return WordCountApp()


# Alternative: if the worker expects a specific variable name
mapreduce_app = WordCountApp()
