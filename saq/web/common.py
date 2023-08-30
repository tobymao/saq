import html
import pathlib
from typing import Any

from saq.job import Job

STATIC_PATH = pathlib.Path(__file__).parent.resolve() / "static"
BODY = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="stylesheet" type="text/css" href="{root_path}/static/pico.min.css.gz">
        <title>SAQ</title>
    </head>
    <body>
        <div id="app"></div>
        <script>const root_path = "{root_path}";</script>
        <script src="{root_path}/static/snabbdom.js.gz"></script>
        <script src="{root_path}/static/app.js"></script>
    </body>
</html>
""".strip()


def render(**kwargs: Any) -> str:
    return BODY.format(**{k: html.escape(v) for k, v in kwargs.items()})


def job_dict(job: Job) -> dict:
    data = job.to_dict()
    if "kwargs" in data:
        data["kwargs"] = repr(data["kwargs"])
    if "result" in data:
        data["result"] = repr(data["result"])
    return data
