#!/bin/bash
pipenv install --dev pytest pylint coverage tox pytest-cov sphinx sphinx-rtd-theme && pipenv shell
