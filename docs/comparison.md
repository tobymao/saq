# Comparison

## Comparison to ARQ
SAQ is heavily inspired by [ARQ](https://github.com/samuelcolvin/arq) but has several enhancements.

1. Avoids polling by leveraging [BLMOVE](https://redis.io/commands/blmove) or [RPOPLPUSH](https://redis.io/commands/rpoplpush) and NOTIFY
    1. SAQ has much lower latency than ARQ, with delays of < 5ms. ARQ's default polling frequency is 0.5 seconds
	  2. SAQ is up to [8x faster](#benchmarks) than ARQ
2. Web interface for monitoring queues and workers
3. Heartbeat monitor for abandoned jobs
4. More robust failure handling
    1. Storage of stack traces
    2. Sweeping stuck jobs
    3. Handling of cancelled jobs different from failed jobs (machine redeployments)
5. Before and after job hooks
6. Easily run multiple workers to leverage more cores
