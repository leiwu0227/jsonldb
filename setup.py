from setuptools import setup, find_packages

setup(
    name="jsonldb",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "typing>=3.7.4",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="A Python package for efficient JSONL file operations with indexing support",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/jsonldb",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
) 