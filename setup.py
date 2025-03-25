from setuptools import setup, find_packages

setup(
    name="jsonlfile",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "typing>=3.7.4",
        "jsonlines>=3.1.0",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="A Python package for efficient JSONL file operations with byte-position indexing",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/jsonlfile",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: File Formats :: JSON",
    ],
    python_requires=">=3.7",
    keywords="jsonl, file storage, index, json lines",
) 