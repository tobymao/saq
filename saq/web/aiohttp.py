"""
Built-in AIOHttp webserver, activated with --web param to worker.
"""

from __future__ import annotations

import asyncio
import json
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
WS_CLIENTS_KEY = web.AppKey("ws_clients", set)
WS_TASK_KEY = web.AppKey("_ws_task", asyncio.Task)


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


async def job_list(request: Request) -> Response:
    """List jobs with optional filtering."""
    queue_name = request.match_info.get("queue", "")
    queue = _get_queue(request, queue_name)

    from saq.job import Status as _Status

    statuses_str = request.query.get("status", "")
    statuses = None
    if statuses_str:
        statuses = [_Status(s.strip()) for s in statuses_str.split(",") if s.strip()]

    function = request.query.get("function")
    offset = int(request.query.get("offset", "0"))
    limit = min(int(request.query.get("limit", "100")), 1000)

    jobs = await queue.list_jobs(
        statuses=statuses,
        function=function,
        offset=offset,
        limit=limit,
    )
    return web.json_response({"jobs": [job_dict(j) for j in jobs]})


async def batch_retry(request: Request) -> Response:
    """Batch retry multiple jobs."""
    body = await request.json()
    keys = body.get("keys")
    if keys is None:
        return web.json_response({"error": "keys is required"}, status=400)

    queue_name = request.match_info.get("queue", "")
    queue = _get_queue(request, queue_name)
    result = await queue.batch_retry(keys)
    return web.json_response(result)


async def batch_abort(request: Request) -> Response:
    """Batch abort multiple jobs."""
    body = await request.json()
    keys = body.get("keys")
    if keys is None:
        return web.json_response({"error": "keys is required"}, status=400)

    queue_name = request.match_info.get("queue", "")
    queue = _get_queue(request, queue_name)
    result = await queue.batch_abort(keys)
    return web.json_response(result)


async def views(_request: Request) -> Response:
    return web.Response(text=render(root_path=""), content_type="text/html")


async def websocket(request: Request) -> web.WebSocketResponse:
    """WebSocket endpoint for real-time queue updates."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    clients = request.app[WS_CLIENTS_KEY]
    clients.add(ws)
    try:
        async for msg in ws:
            pass  # We only push, don't read
    finally:
        clients.discard(ws)

    return ws


async def _ws_broadcast(app: Application) -> None:
    """Background task: push queue info to all WS clients every 2 seconds."""
    try:
        while True:
            clients: set[web.WebSocketResponse] = app[WS_CLIENTS_KEY]
            if clients:
                queues = app[QUEUES_KEY]
                data = {"queues": [await q.info() for q in queues.values()]}
                payload = json.dumps(data)
                closed = []
                for ws in clients:
                    try:
                        await ws.send_str(payload)
                    except ConnectionResetError:
                        closed.append(ws)
                for ws in closed:
                    clients.discard(ws)
            await asyncio.sleep(2)
    except asyncio.CancelledError:
        pass


async def _start_broadcast(app: Application) -> None:
    app[WS_CLIENTS_KEY] = set()
    app[WS_TASK_KEY] = asyncio.create_task(_ws_broadcast(app))


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
    if "/api/" in request.path:
        try:
            resp = await handler(request)
            return resp
        except Exception:
            error = traceback.format_exc()
            logging.error(error)
            return web.json_response({"error": error})
    return await handler(request)


async def shutdown(app: Application) -> None:
    ws_task = app.get(WS_TASK_KEY)
    if ws_task:
        ws_task.cancel()
    for ws in app.get(WS_CLIENTS_KEY, set()):
        await ws.close()
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
            web.get("/ws", websocket),
            web.post("/api/queues/{queue}/jobs/batch/retry", batch_retry),
            web.post("/api/queues/{queue}/jobs/batch/abort", batch_abort),
            web.get("/api/queues/{queue}/jobs", job_list),
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
    app.on_startup.append(_start_broadcast)
    app.on_shutdown.append(shutdown)
    return app
