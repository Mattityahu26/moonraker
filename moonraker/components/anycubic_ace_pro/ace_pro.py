import asyncio
import json
import logging
import os
from typing import Optional

from aiohttp import web

LOG = logging.getLogger("ace_pro")

UNIX_SOCKET_PATH = "/tmp/unix_uds1"
ETX = b"\x03"
DEFAULT_TIMEOUT = 10.0

# -------------------------------
# Helper: JSON-RPC IPC over unix socket
# -------------------------------
async def send_to_socket(payload: dict, timeout: float = DEFAULT_TIMEOUT):
    data = json.dumps(payload).encode() + ETX
    reader = writer = None
    try:
        reader, writer = await asyncio.open_unix_connection(UNIX_SOCKET_PATH)
    except Exception as e:
        LOG.exception("ace_pro: Cannot open unix socket %s", UNIX_SOCKET_PATH)
        raise web.HTTPServiceUnavailable(reason=f"Cannot open socket: {e}")

    try:
        writer.write(data)
        await writer.drain()

        buf = bytearray()
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        try:
            while True:
                if loop.time() > deadline:
                    raise asyncio.TimeoutError("Timeout waiting for response")
                chunk = await asyncio.wait_for(reader.read(1024), timeout=1.0)
                if not chunk:
                    break
                buf.extend(chunk)
                if ETX in chunk:
                    break
        except asyncio.TimeoutError:
            LOG.warning("ace_pro: timeout waiting for response from backend")
            raise web.HTTPGatewayTimeout(reason="Timeout waiting for backend")

        if ETX in buf:
            buf = buf[: buf.index(ETX)]

        text = buf.decode("utf-8", errors="replace").strip()
        if not text:
            return {}

        try:
            return json.loads(text)
        except Exception:
            LOG.exception("ace_pro: invalid JSON from backend: %r", text)
            raise web.HTTPBadGateway(reason="Invalid JSON from backend")
    finally:
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

# -------------------------------
# Helper: Build JSON-RPC payload
# -------------------------------
def rpc(method: str, params: dict = None, req_id: Optional[int] = None):
    if req_id is None:
        req_id = int(asyncio.get_event_loop().time() * 1000) % 2**31
    return {"method": method, "params": params or {}, "id": req_id}

# -------------------------------
# HTTP route handlers
# -------------------------------
async def ace_status(request):
    payload = {
        "method": "objects/query",
        "params": {"objects": {"filament_hub": None}},
        "id": int(asyncio.get_event_loop().time() * 1000),
    }
    resp = await send_to_socket(payload)
    return web.json_response(resp)

async def ace_get_config(request):
    payload = rpc("filament_hub/get_config", {})
    resp = await send_to_socket(payload)
    return web.json_response(resp)

async def ace_set_config(request):
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(reason="Invalid JSON body")
    payload = rpc("filament_hub/set_config", body)
    resp = await send_to_socket(payload)
    return web.json_response(resp)

async def ace_start_drying(request):
    try:
        body = await request.json()
        params = {
            "id": body["id"],
            "duration": int(body.get("duration", 3600)),
            "temp": int(body.get("temp", 45)),
            #"fan_speed": int(body.get("fan_speed", 0)),
        }
    except KeyError:
        raise web.HTTPBadRequest(reason="Missing required key 'id'")
    except Exception:
        raise web.HTTPBadRequest(reason="Invalid JSON body or invalid parameter types")
    payload = rpc("filament_hub/start_drying", params)
    resp = await send_to_socket(payload)
    return web.json_response(resp)

async def ace_stop_drying(request):
    try:
        body = await request.json()
        params = {"id": body["id"]}
    except KeyError:
        raise web.HTTPBadRequest(reason="Missing required key 'id'")
    except Exception:
        raise web.HTTPBadRequest(reason="Invalid JSON body")
    payload = rpc("filament_hub/stop_drying", params)
    resp = await send_to_socket(payload)
    return web.json_response(resp)

async def ace_set_filament(request):
    try:
        body = await request.json()
        required = {"id", "index"}
        if not required.issubset(body.keys()):
            raise web.HTTPBadRequest(reason="Missing keys: 'id' and 'index'")
        params = {"id": int(body["id"]), "index": int(body["index"])}
        if "type" in body:
            params["type"] = str(body["type"])
        if "color" in body:
            params["color"] = str(body["color"])
    except web.HTTPError:
        raise
    except KeyError:
        raise web.HTTPBadRequest(reason="Missing keys: 'id' and 'index'")
    except Exception:
        raise web.HTTPBadRequest(reason="Invalid JSON body or invalid parameter types")
    payload = rpc("filament_hub/set_filament_info", params)
    resp = await send_to_socket(payload)
    return web.json_response(resp)

# -------------------------------
# Setup function called by Moonraker
# -------------------------------
def setup_module(config):
    LOG.info("ace_pro: setting up plugin")
    # allow config under components.anycubic_ace_pro or top-level anycubic_ace_pro
    comp_cfg = {}
    try:
        comp_cfg = (config.get("components") or {}).get("anycubic_ace_pro", config.get("anycubic_ace_pro", {}))
    except Exception:
        comp_cfg = config or {}

    global UNIX_SOCKET_PATH, DEFAULT_TIMEOUT
    UNIX_SOCKET_PATH = comp_cfg.get("socket_path", UNIX_SOCKET_PATH)
    try:
        DEFAULT_TIMEOUT = float(comp_cfg.get("timeout", DEFAULT_TIMEOUT))
    except Exception:
        LOG.warning("ace_pro: invalid timeout in config, using default %s", DEFAULT_TIMEOUT)

    web_app = config.get("web_app") or config.get("app") or config.get("aiohttp_app")
    if not web_app:
        LOG.error("ace_pro: cannot find web app to register routes")
        return

    # early validation of socket path and permissions
    try:
        if not os.path.exists(UNIX_SOCKET_PATH):
            LOG.warning("ace_pro: unix socket %s does not exist at setup time", UNIX_SOCKET_PATH)
        else:
            # check read/write permission for the current process user where possible
            try:
                if not os.access(UNIX_SOCKET_PATH, os.R_OK | os.W_OK):
                    LOG.warning(
                        "ace_pro: no read/write access to unix socket %s for current user. Moonraker may not be able to connect.",
                        UNIX_SOCKET_PATH,
                    )
            except Exception:
                LOG.debug("ace_pro: unable to validate socket permissions for %s", UNIX_SOCKET_PATH)
    except Exception:
        LOG.exception("ace_pro: failed to check unix socket path %s", UNIX_SOCKET_PATH)

    web_app.router.add_get("/printer/ace/status", ace_status)
    web_app.router.add_get("/printer/ace/config", ace_get_config)
    web_app.router.add_post("/printer/ace/config", ace_set_config)
    web_app.router.add_post("/printer/ace/start_drying", ace_start_drying)
    web_app.router.add_post("/printer/ace/stop_drying", ace_stop_drying)
    web_app.router.add_post("/printer/ace/set_filament", ace_set_filament)
    LOG.info("ace_pro: routes registered under /printer/ace/*")
    