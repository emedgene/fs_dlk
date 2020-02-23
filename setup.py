#!/usr/bin/env python

from setuptools import setup, find_packages

with open("fs_dlk/_version.py") as f:
    exec(f.read())

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3.3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Topic :: System :: Filesystems",
]

with open("README.md", "rt") as f:
    DESCRIPTION = f.read()

REQUIREMENTS = ["azure-datalake-store~=0.0", "fs~=2.4", "six~=1.14"]

setup(
    name="fs-dlk",
    author="emedgene",
    classifiers=CLASSIFIERS,
    description="Azure Datalake filesystem for PyFilesystem2",
    install_requires=REQUIREMENTS,
    license="MIT",
    long_description=DESCRIPTION,
    packages=find_packages(),
    keywords=["pyfilesystem", "Azure", "Datalake", "dlk"],
    platforms=["any"],
    url="https://github.com/emedgene/fs_dlk",
    download_url="https://github.com/emedgene/fs_dlk/tarball/feature/doc",
    version=__version__,
    entry_points={"fs.opener": ["dlk = fs_dlk.opener:DLKFSOpener"]},
)
