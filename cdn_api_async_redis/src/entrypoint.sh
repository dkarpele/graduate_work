#!/bin/sh
echo "Waiting for volume $APP_HOME/tests/ ..."

while [ ! -d "$APP_HOME"/tests/ ]
do
  echo "volume $APP_HOME/tests/ is still creating ..."
  sleep 1
done

echo "volume $APP_HOME/tests/ created"

pip install -r "$APP_HOME"/tests/functional/requirements.txt &&
python3 "$APP_HOME"/tests/functional/waiters.py &&
pytest -s -v "$APP_HOME"/tests/functional/src
