import json
import pathlib

from saq.job import Job

STATIC_PATH = pathlib.Path(__file__).parent.resolve() / "static"
BODY = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="stylesheet" type="text/css" href="{root_path}/static/pico.min.css">
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


def render(root_path: str) -> str:
    root_path = root_path.rstrip("/")
    root_path = json.dumps(root_path)[1:-1]
    return BODY.format(root_path=root_path)


def job_dict(job: Job) -> dict:
    return job.to_dict(safe=True)
