from setuptools import setup, find_packages

setup(
    name="jsonlfile",
    version="0.2.0",
    packages=find_packages(),
    install_requires=[
        "typing>=3.7.4",
        "numba>=0.57.0",
        "numpy>=1.20.0",
        "orjson>=3.9.0",
    ],
    author="Lei Wu",
    author_email="leiwu0227@gmail.com   ",
    description="A Python package for efficient JSONL file operations with byte-position indexing",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/leiwu0227/jsonldb",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: File Formats :: JSON",
        "Intended Audience :: Developers",
        "Development Status :: 4 - Beta",
    ],
    python_requires=">=3.7",
    keywords="jsonl, file storage, index, json lines, high performance, numba",
) 