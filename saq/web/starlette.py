"""
Starlette/FastAPI/ASGI
"""

from __future__ import annotations

import os

from starlette.applications import Starlette
from starlette.datastructures import Headers
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response
from starlette.routing import Mount, Route
from starlette.staticfiles import NotModifiedResponse, PathLike, StaticFiles
from starlette.types import Scope

from saq.job import Job
from saq.queue import Queue
from saq.types import QueueInfo
from saq.web.common import STATIC_PATH, job_dict, render

QUEUES: dict[str, Queue] = {}
ROOT_PATH: str = ""


class GZStaticFiles(StaticFiles):
    def file_response(
        self,
        full_path: PathLike,
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        method = scope["method"]
        request_headers = Headers(scope=scope)

        response = FileResponse(
            full_path, status_code=status_code, stat_result=stat_result, method=method
        )
        # By default, the starlette StaticFiles handler doesn't handle pre-compressed files, but we explicitly want it to.
        if str(full_path).endswith(".gz"):
            response.headers.setdefault("Content-Encoding", "gzip")
        if self.is_not_modified(response.headers, request_headers):
            return NotModifiedResponse(response.headers)
        return response


async def views(request: Request) -> Response:  # pylint: disable=unused-argument
    return Response(content=render(root_path=ROOT_PATH), media_type="text/html")


async def health(request: Request) -> Response:  # pylint: disable=unused-argument
    if await _get_all_info():
        return Response(content="OK", media_type="text/plain")
    raise HTTPException(status_code=500)


async def queues_(request: Request) -> JSONResponse:
    queue_name = request.path_params.get("queue")

    response: dict[str, QueueInfo | list[QueueInfo]] = {}

    if queue_name:
        response["queue"] = await _get_queue(queue_name).info(jobs=True)
    else:
        response["queues"] = await _get_all_info()

    return JSONResponse(response)


async def jobs(request: Request) -> JSONResponse:
    queue_name = request.path_params["queue"]
    job_key = request.path_params["job"]

    job = await _get_job(queue_name, job_key)
    return JSONResponse({"job": job_dict(job)})


async def retry(request: Request) -> JSONResponse:
    queue_name = request.path_params["queue"]
    job_key = request.path_params["job"]

    job = await _get_job(queue_name, job_key)
    await job.retry("retried from ui")
    return JSONResponse({})


async def abort(request: Request) -> JSONResponse:
    queue_name = request.path_params["queue"]
    job_key = request.path_params["job"]

    job = await _get_job(queue_name, job_key)
    await job.abort("aborted from ui")
    return JSONResponse({})


async def _get_all_info() -> list[QueueInfo]:
    return [await q.info() for q in QUEUES.values()]


def _get_queue(queue_name: str) -> Queue:
    return QUEUES[queue_name]


async def _get_job(queue_name: str, job_key: str) -> Job:
    job = await _get_queue(queue_name).job(job_key)
    if not job:
        raise ValueError(f"Job {job_key} not found")
    return job


def saq_web(root_path: str, queues: list[Queue]) -> Starlette:
    """
    Create an embeddable monitoring Web UI

    Example:
        .. code-block::

            routes = [
                Mount("/monitor", saq_web("/monitor", queues=all_the_queues_list))
            ]

    Args:
        root_path: The absolute mount point, typically the same as where you mount it.
        queues: The list of known queues

    Returns:
        Starlette ASGI instance.
    """
    global ROOT_PATH  # pylint: disable=global-statement

    QUEUES.clear()
    for queue in queues:
        QUEUES[queue.name] = queue
    ROOT_PATH = root_path

    return Starlette(
        routes=[
            Route("/", views),
            Route("/queues/{queue}", views),
            Route("/queues/{queue}/jobs/{job}", views),
            Route("/api/queues", queues_),
            Route("/api/queues/{queue}", queues_),
            Route("/api/queues/{queue}/jobs/{job}", jobs),
            Route("/api/queues/{queue}/jobs/{job}/retry", retry, methods=["POST"]),
            Route("/api/queues/{queue}/jobs/{job}/abort", abort, methods=["POST"]),
            Mount("/static", GZStaticFiles(directory=STATIC_PATH)),
            Route("/health", health),
        ]
    )
