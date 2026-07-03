import asyncio
import os
import random
import time

from aiohttp import web

DELAY_MS = float(os.environ.get("MOCK_DELAY_MS", "0"))
FAIL_RATE = float(os.environ.get("MOCK_FAIL_RATE", "0"))

store = []
store_lock = asyncio.Lock()


async def handle_io_value(request: web.Request) -> web.Response:
    topic = request.match_info["topic"]
    value = request.match_info["value"]

    if DELAY_MS > 0:
        await asyncio.sleep(DELAY_MS / 1000.0)
    if FAIL_RATE > 0 and random.random() < FAIL_RATE:
        return web.Response(status=503, text="simulated failure")

    async with store_lock:
        store.append({"topic": topic, "value": value, "ts": time.time()})
    return web.Response(status=200, text="OK")


async def handle_io_novalue(request: web.Request) -> web.Response:
    return web.Response(status=200, text="OK")


async def handle_stats(request: web.Request) -> web.Response:
    async with store_lock:
        count = len(store)
    return web.json_response({"count": count})


async def handle_export(request: web.Request) -> web.Response:
    async with store_lock:
        data = list(store)
    return web.json_response(data)


async def handle_reset(request: web.Request) -> web.Response:
    async with store_lock:
        store.clear()
    return web.Response(status=200, text="reset")


app = web.Application()
app.router.add_get("/dev/sps/io/{topic}/{value}", handle_io_value)
app.router.add_get("/dev/sps/io/{topic}/", handle_io_novalue)
app.router.add_get("/_stats", handle_stats)
app.router.add_get("/_export", handle_export)
app.router.add_post("/_reset", handle_reset)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=8080)
