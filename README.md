# SAQ
SAQ (Simple Async Queue) is a simple and performant job queueing framework built on top of asyncio and redis.

## Development
```
python -m venv env
source env/bin/activate
pip install -e .
docker run -p 6379:6379 redis
./run\_checks.sh
```
