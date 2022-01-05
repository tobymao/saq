import html
import logging
import os
import pathlib
import traceback

from aiohttp import web
from saq.queue import Queue


static = os.path.join(pathlib.Path(__file__).parent.resolve(), "static")

body = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="stylesheet" type="text/css" href="/static/pico.min.css.gz">
        <title>SAQ</title>
    </head>
    <body>
        <div id="app"></div>
        <script src="/static/snabbdom.js.gz"></script>
        <script src="/static/app.js"></script>
    </body>
</html>
""".strip()


def render(**kwargs):
    return body.format(**{k: html.escape(v) for k, v in kwargs.items()})


async def queues(request):
    queue = request.match_info.get("queue")
    info = await app["queue"].info(queue=queue, jobs=queue)
    response = {}

    if queue:
        response["queue"] = info[queue]
    else:
        response["queues"] = list(info.values())

    return web.json_response(response)


async def jobs(request):
    job = await _get_job(request)
    return web.json_response({"job": job.to_dict()})


async def retry(request):
    job = await _get_job(request)
    await job.retry("retried from ui")
    return web.json_response({})


async def abort(request):
    job = await _get_job(request)
    await job.abort("aborted from ui")
    return web.json_response({})


async def views(_request):
    return web.Response(text=render(), content_type="text/html")


async def _get_job(request):
    job_key = request.match_info.get("job")
    job = await app["queue"].job(f"saq:job:{job_key}")
    if not job:
        raise ValueError(f"Job {job_key} not found")
    return job


async def redis_queue(app_):
    app_["queue"] = Queue.from_url("redis://localhost")
    yield
    await app_["queue"].disconnect()


@web.middleware
async def exceptions(request, handler):
    if request.path.startswith("/api"):
        try:
            resp = await handler(request)
            return resp
        except Exception:
            error = traceback.format_exc()
            logging.error(error)
            return web.json_response({"error": error})
    return await handler(request)


app = web.Application(middlewares=[exceptions])

app.add_routes(
    [
        web.static("/static", static, append_version=True),
        web.get("/api/jobs/{job}", jobs),
        web.post("/api/jobs/{job}/retry", retry),
        web.post("/api/jobs/{job}/abort", abort),
        web.get("/api/queues", queues),
        web.get("/api/queues/{queue}", queues),
        web.get("/", views),
        web.get("/queues/{queue}", views),
        web.get("/jobs/{job}", views),
    ]
)
app.cleanup_ctx.append(redis_queue)
