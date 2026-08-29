from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, Response
import httpx
import os

app = FastAPI()

# CORS: allow Janitor (browser) to hit this proxy without restrictions
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

REWIND_BASE = "https://api.rewind.ai"
TIMEOUT = httpx.Timeout(300.0, connect=10.0)

# Optional: Rewind has inconsistent trailing slashes. Normalize the ones we know.
TRAILING_SLASH = {"v1/chat/completions", "v1/tts", "v1/stt"}

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def proxy(request: Request, path: str):
    # Fix trailing slash quirks so Rewind doesn't 404 us
    if path in TRAILING_SLASH:
        path += "/"

    target_url = f"{REWIND_BASE}/{path}"

    # Pass through headers except hop-by-hop stuff
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in {"host", "content-length", "content-encoding", "transfer-encoding"}
    }

    body = await request.body()
    client = httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True)

    try:
        req = client.build_request(
            method=request.method,
            url=target_url,
            headers=headers,
            content=body,
            params=request.query_params,
        )
        response = await client.send(req, stream=True)

        # Strip hop-by-hop response headers
        response_headers = {
            k: v for k, v in response.headers.items()
            if k.lower() not in {"content-encoding", "transfer-encoding", "content-length"}
        }

        media_type = response.headers.get("content-type", "application/json")

        async def stream_response():
            async for chunk in response.aiter_raw():
                yield chunk
            await response.aclose()
            await client.aclose()

        return StreamingResponse(
            stream_response(),
            status_code=response.status_code,
            headers=response_headers,
            media_type=media_type,
        )

    except Exception as e:
        await client.aclose()
        return Response(
            content=f'{{"error": "{str(e)}"}}'.encode(),
            status_code=502,
            media_type="application/json",
        )


@app.get("/health")
async def health():
    return {"status": "ok", "proxy": "rewind-ai", "timeout": 300}
