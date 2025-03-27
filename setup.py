from setuptools import setup, find_packages

setup(
    name="jsonldb",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.0",
        "orjson>=3.6.0",
        "numba>=0.54.0",
        "gitpython>=3.1.0",
        "bokeh>=2.0.0",
        "numpy>=1.20.0"
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="A simple file-based database that stores data in JSONL format with version control and visualization capabilities",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/jsonldb",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    include_package_data=True,
    package_data={
        "jsonldb": ["*.py"],
    },
    entry_points={
        "console_scripts": [
            "jsonldb=jsonldb.cli:main",
        ],
    },
) 