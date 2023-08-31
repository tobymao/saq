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

(benchmarks)=
## Benchmarks

```nasm
pip install arq && python benchmarks/simple.py arq
pip install saq && python benchmarks/simple.py saq
pip install rq && python benchmarks/simple.py rq
```

### Results
N = 1000, Results in seconds

| Workflow |      saq | [arq](https://github.com/samuelcolvin/arq) | [rq](https://github.com/rq/rq) |
|:---------|---------:|-------------------------------------------:|-------------------------------:|
| enqueue  |  0.17970 |                                    0.54293 |                        0.48645 |
| noop     |  0.91998 |                                    5.00900 |                        14.9416 |
| sleep    |  2.82186 |                                    5.01430 |                        58.9042 |
