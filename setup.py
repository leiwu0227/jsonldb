from setuptools import setup, find_packages


setup(
    name="jsonldb",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.0",
        "orjson>=3.6.0",
        "numpy>=1.20.0",
    ],
    author="Lei Wu",
    author_email="leiwu0227@gmail.com",
    description="A file-based database for keyed records stored in JSONL format",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/leiwu0227/jsonldb",
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
)
