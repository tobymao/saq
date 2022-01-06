# Benchmarks

```
pip install arq && python benchmarks/simple.py arq
pip install saq && python benchmarks/simple.py saq
pip install rq && python benchmarks/simple.py rq
```

## Results
N = 1000, Results in seconds

| Workflow | saq      | [arq](https://github.com/samuelcolvin/arq) | [rq](https://github.com/rq/rq) |
| -------- | -------- | ------------------------------------------ | ------------------------------ |
| enqueue  | 0.14231  | 0.36990                                    | 0.30486                        |
| noop     | 0.60916  | 5.01813                                    | 7.11374                        |
| sleep    | 2.71951  | 5.01549                                    | 50.1402                        |
