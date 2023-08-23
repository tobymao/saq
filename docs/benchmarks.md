# Benchmarks

```nasm
pip install arq && python benchmarks/simple.py arq
pip install saq && python benchmarks/simple.py saq
pip install rq && python benchmarks/simple.py rq
```

## Results
N = 1000, Results in seconds

| Workflow |      saq | [arq](https://github.com/samuelcolvin/arq) | [rq](https://github.com/rq/rq) |
|:---------|---------:|-------------------------------------------:|-------------------------------:|
| enqueue  |  0.17970 |                                    0.54293 |                        0.48645 |
| noop     |  0.91998 |                                    5.00900 |                        14.9416 |
| sleep    |  2.82186 |                                    5.01430 |                        58.9042 |
