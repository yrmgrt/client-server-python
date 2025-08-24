import os
import fastapi
from pathlib import Path
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

# from fastapi_profiler import PyInstrumentProfilerMiddleware

from router.endpoints import router as api_endpoint_router
from config.events import (
    execute_backend_server_event_handler,
    start_scheduler,
    stop_scheduler,
    terminate_backend_server_event_handler,
)
from config.manager import settings
import sys
import socket
import signal


ip_addr = socket.gethostbyname(socket.gethostname())
print(ip_addr)
if settings.CHECK_HOST and ip_addr not in [
    "192.168.1.78",
    "192.168.1.81",
    "192.168.1.206",
    "192.168.1.139",
    "192.168.1.178",
]:
    exit()


def proc_exit_on_signal(signal_number, frame):
    proc_id = os.getpid()
    print(
        f"Received signal {signal_number} for process {proc_id}, exiting gracefully..."
    )
    os._exit(255)


signal.signal(signal.SIGINT, proc_exit_on_signal)
signal.signal(signal.SIGTERM, proc_exit_on_signal)


def initialize_backend_application() -> fastapi.FastAPI:
    app = fastapi.FastAPI(**settings.set_backend_app_attributes)  # type: ignore

    # app.add_middleware(
    #     PyInstrumentProfilerMiddleware,
    #     server_app=app,
    #     is_print_each_request=False,
    #     open_in_browser=True,
    #     profiler_output_type="html",
    # )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.ALLOWED_ORIGINS,
        allow_methods=settings.ALLOWED_METHODS,
        allow_headers=settings.ALLOWED_HEADERS,
        allow_credentials=True
    )
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    app.add_event_handler(
        "startup",
        execute_backend_server_event_handler(backend_app=app),
    )

    app.add_event_handler(
        "startup",
        start_scheduler,
    )

    app.add_event_handler(
        "shutdown",
        terminate_backend_server_event_handler(backend_app=app),
    )

    app.add_event_handler(
        "shutdown",
        stop_scheduler,
    )

    app.include_router(router=api_endpoint_router, prefix=settings.API_PREFIX)

    return app


backend_app: fastapi.FastAPI = initialize_backend_application()

if __name__ == "__main__":

    args = {
        "host": settings.SERVER_HOST,
        "port": settings.SERVER_PORT,
        "log_level": settings.LOGGING_LEVEL,
    }

    if "python" in Path(sys.executable).name:
        app: str | fastapi.FastAPI = "main:backend_app"
        args.update({"reload": settings.DEBUG, "workers": settings.SERVER_WORKERS})
    else:
        app = backend_app

    args.update({"app": app})

    uvicorn.run(**args)
