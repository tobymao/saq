# Benchmarks

```
pip install arq && python benchmarks/simple.py arq
pip install saq && python benchmarks/simple.py saq
pip install saq && python benchmarks/simple.py saq_pg
pip install rq && python benchmarks/simple.py rq
```

## Results
N = 1000, Results in seconds

| Workflow | saq      | saq pg  | [arq](https://github.com/samuelcolvin/arq) | [rq](https://github.com/rq/rq) |
| -------- | -------- | ------- | ------------------------------------------ | ----------------------------- |
| enqueue  | 0.09466  | 0.50254 | 0.15670                                    | 0.21894                       |
| noop     | 0.40511  | 1.02754 | 5.02181                                    | 15.0959                       |
| sleep    | 2.92441  | 3.14007 | 29.5913                                    | 80.0692                       |
