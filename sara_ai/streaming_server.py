import asyncio
import websockets
from sara_ai.logging_utils import log_event
from sara_ai.sentry_utils import init_sentry

init_sentry()
trace_id = log_event(service="streaming_server", event="startup", status="ok", message="Streaming server starting")

async def handler(websocket, path):
    try:
        log_event(service="streaming_server", event="connection", status="ok", message="Client connected", trace_id=trace_id)
        async for message in websocket:
            log_event(service="streaming_server", event="message_received", status="ok", message=message, trace_id=trace_id)
            await websocket.send("ACK")
    except Exception as e:
        log_event(service="streaming_server", event="error", status="failed", message=str(e), level="ERROR", trace_id=trace_id)

if __name__ == "__main__":
    start_server = websockets.serve(handler, "0.0.0.0", 6789)
    asyncio.get_event_loop().run_until_complete(start_server)
    log_event(service="streaming_server", event="listening", status="ok", message="Listening on port 6789", trace_id=trace_id)
    asyncio.get_event_loop().run_forever()
