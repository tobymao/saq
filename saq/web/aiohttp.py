"""
Built-in AIOHttp webserver, activated with --web param to worker.
"""

from __future__ import annotations

import logging
import os
import traceback
import typing as t

from aiohttp import web

from saq.queue import Queue
from saq.web.common import STATIC_PATH, job_dict, render

if t.TYPE_CHECKING:
    from aiohttp.typedefs import Handler
    from aiohttp.web import StreamResponse
    from aiohttp.web_app import Application
    from aiohttp.web_request import Request
    from aiohttp.web_response import Response

    from saq.job import Job
    from saq.types import QueueInfo


QUEUES_KEY = web.AppKey("queues", t.Dict[str, Queue])


async def queues_(request: Request) -> Response:
    queue_name = request.match_info.get("queue")

    response: dict[str, QueueInfo | list[QueueInfo]] = {}

    if queue_name:
        response["queue"] = await _get_queue(request, queue_name).info(jobs=True)
    else:
        response["queues"] = await _get_all_info(request)

    return web.json_response(response)


async def jobs(request: Request) -> Response:
    job = await _get_job(request)
    return web.json_response({"job": job_dict(job)})


async def retry(request: Request) -> Response:
    job = await _get_job(request)
    await job.retry("retried from ui")
    return web.json_response({})


async def abort(request: Request) -> Response:
    job = await _get_job(request)
    await job.abort("aborted from ui")
    return web.json_response({})


async def views(_request: Request) -> Response:
    return web.Response(text=render(root_path=""), content_type="text/html")


async def health(request: Request) -> Response:
    if await _get_all_info(request):
        return web.Response(text="OK")
    raise web.HTTPInternalServerError


async def _get_all_info(request: Request) -> list[QueueInfo]:
    return [await q.info() for q in request.app[QUEUES_KEY].values()]


def _get_queue(request: Request, queue_name: str) -> Queue:
    return request.app[QUEUES_KEY][queue_name]


async def _get_job(request: Request) -> Job:
    queue_name = request.match_info.get("queue", "")
    job_key = request.match_info.get("job", "")

    job = await _get_queue(request, queue_name).job(job_key)
    if not job:
        raise ValueError(f"Job {job_key} not found")
    return job


@web.middleware
async def exceptions(request: Request, handler: Handler) -> StreamResponse:
    if request.path.startswith("/api"):
        try:
            resp = await handler(request)
            return resp
        except Exception:
            error = traceback.format_exc()
            logging.error(error)
            return web.json_response({"error": error})
    return await handler(request)


async def shutdown(app: Application) -> None:
    for queue in app.get(QUEUES_KEY, {}).values():
        await queue.disconnect()


def create_app(queues: list[Queue]) -> Application:
    middlewares = [exceptions]
    password = os.environ.get("AUTH_PASSWORD")

    if password:
        from aiohttp_basicauth import BasicAuthMiddleware  # type: ignore

        user = os.environ.get("AUTH_USER", "admin")
        middlewares.append(BasicAuthMiddleware(username=user, password=password))

    app = web.Application(middlewares=middlewares)
    app[QUEUES_KEY] = {q.name: q for q in queues}

    app.add_routes(
        [
            web.static("/static", STATIC_PATH, append_version=True),
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
