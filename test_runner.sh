#!/bin/bash

cd tests
PYTHONPATH=".." pytest --cov=fs_dlk .
