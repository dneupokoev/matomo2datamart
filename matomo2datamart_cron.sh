#!/bin/bash
cd /opt/dix/matomo2datamart/
PATH="/opt/dix/matomo2datamart/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
VIRTUAL_ENV="/opt/dix/matomo2datamart/.venv"

pipenv run python3 matomo2datamart.py
