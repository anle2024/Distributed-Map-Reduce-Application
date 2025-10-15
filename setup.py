#!/usr/bin/env python3
"""
Setup script for distributed MapReduce package.
"""

from setuptools import setup, find_packages
import os

# Read README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [
        line.strip() for line in fh if line.strip() and not line.startswith("#")
    ]

setup(
    name="distributed-mapreduce",
    version="1.0.0",
    author="MapReduce Team",
    author_email="team@mapreduce.com",
    description="A distributed MapReduce implementation in Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mapreduce-team/distributed-mapreduce",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black",
            "flake8",
            "mypy",
        ],
        "docs": [
            "matplotlib>=3.5.0",
            "numpy>=1.20.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "mr-coordinator=mapreduce.coordinator:main",
            "mr-worker=mapreduce.worker:main",
        ],
    },
    scripts=[
        "bin/mr-coordinator",
        "bin/mr-worker",
    ],
    include_package_data=True,
    package_data={
        "mapreduce": ["*.py"],
    },
)
