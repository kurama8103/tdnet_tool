#!/usr/bin/env python3
# coding: utf-8
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tdnet_tool",  # Replace with your own username
    version="0.0.2",
    author="kurama8103",
    # author_email="author@example.com",
    description="Get data and download PDF from TDnet.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kurama8103/tdnet_tool",
    packages=setuptools.find_packages(),
    install_requires=["pandas", "bs4"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)