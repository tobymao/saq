# SAQ
SAQ (Simple Async Queue) is a simple and performant job queueing framework built on top of asyncio and redis.

## Install
```
pip install saq
```

## Usage
saq mymodule.settings

## Development
```
python -m venv env
source env/bin/activate
pip install -e .[development,web]
docker run -p 6379:6379 redis
./run_checks.sh
```
