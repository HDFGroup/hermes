#!/bin/bash
coverage run -m pytest
rm -rf "*.pyc"
coverage report
coverage-lcov