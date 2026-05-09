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
        <link rel="stylesheet" type="text/css" href="{root_path}/static/pico.min.css">
        <style>
            .job-table td, .job-table th { vertical-align: middle; }
            .job-table { table-layout: fixed; }
            .action-btns { display: inline-flex; gap: 0.3rem; align-items: center; }
            .action-btns a[role="button"] {
                font-size: 0.7rem; padding: 0.2rem 0.5rem; cursor: pointer;
                border: 1px solid #ccc; border-radius: 3px;
                text-decoration: none; white-space: nowrap;
                background-color: #f5f5f5; color: #333;
            }
            .action-btns a[role="button"].btn-danger {
                border-color: #d81b60; background-color: #d81b60; color: #fff;
            }
            .ws-dot {
                display: inline-block; width: 8px; height: 8px;
                border-radius: 50%; margin-left: 0.5rem;
            }
            .ws-dot.connected { background-color: #2e7d32; }
            .ws-dot.disconnected { background-color: #bbb; }
        </style>
        <title>SAQ</title>
    </head>
    <body>
        <div id="app"></div>
        <script>const root_path = "{root_path}";</script>
        <script src="{root_path}/static/snabbdom.js"></script>
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
