import html
import logging
import os
import pathlib
import traceback

from aiohttp import web

from saq import Job


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


async def queues_(request):
    queue_name = request.match_info.get("queue")

    response = {}

    if queue_name:
        response["queue"] = await _get_queue(request, queue_name).info(jobs=True)
    else:
        response["queues"] = await _get_all_info(request)

    return web.json_response(response)


async def jobs(request):
    job = await _get_job(request)
    job_dict = job.to_dict()
    if "kwargs" in job_dict:
        job_dict["kwargs"] = repr(job_dict["kwargs"])
    if "result" in job_dict:
        job_dict["result"] = repr(job_dict["result"])
    return web.json_response({"job": job_dict})


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


async def health(request):
    if await _get_all_info(request):
        return web.Response(text="OK")
    raise web.HTTPInternalServerError


async def _get_all_info(request):
    return [await q.info() for q in request.app["queues"].values()]


def _get_queue(request, queue_name):
    return request.app["queues"][queue_name]


async def _get_job(request):
    queue_name = request.match_info.get("queue")
    job_key = request.match_info.get("job")

    job = await _get_queue(request, queue_name).job(
        Job.id_from_key(job_key, queue_name)
    )
    if not job:
        raise ValueError(f"Job {job_key} not found")
    return job


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


async def shutdown(app):
    for queue in app.get("queues", {}).values():
        await queue.disconnect()


def create_app(queues):
    app = web.Application(middlewares=[exceptions])
    app["queues"] = {q.name: q for q in queues}

    app.add_routes(
        [
            web.static("/static", static, append_version=True),
            web.get("/api/queues/{queue}/jobs/{job}", jobs),
            web.post("/api/queues/{queue}/jobs/{job}/retry", retry),
            web.post("/api/queues/{queue}/jobs/{job}/abort", abort),
            web.get("/api/queues", queues_),
            web.get("/api/queues/{queue}", queues_),
            web.get("/", views),
            web.get("/queues/{queue}", views),
            web.get("/queues/{queue}/jobs/{job}", views),
            web.get("/health", health),
        ]
    )
    app.on_shutdown.append(shutdown)
    return app
