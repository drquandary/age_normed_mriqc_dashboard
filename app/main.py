from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from .routes import router

# Get the directory containing this file
BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Age-Normed MRIQC Dashboard")

# Mount static files
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

# Setup templates
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# Include API routes under /api
app.include_router(router, prefix='/api')


@app.get('/', response_class=HTMLResponse)
async def dashboard(request: Request):
    """Dashboard page."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get('/upload', response_class=HTMLResponse)
async def upload_page(request: Request):
    """Upload page."""
    return templates.TemplateResponse("upload.html", {"request": request})


@app.get('/subjects', response_class=HTMLResponse)
async def subjects_page(request: Request):
    """Subjects list page."""
    return templates.TemplateResponse("subjects.html", {"request": request})


@app.get('/subjects/{subject_id}', response_class=HTMLResponse)
async def subject_detail_page(request: Request, subject_id: str):
    """Subject detail page."""
    return templates.TemplateResponse("subject_detail.html", {
        "request": request,
        "subject_id": subject_id
    })


# WebSocket endpoint for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Main WebSocket endpoint for real-time updates."""
    # Import here to avoid circular imports
    from .routes import manager
    import json
    
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "ping":
                await websocket.send_text(json.dumps({
                    "type": "pong",
                    "timestamp": "now"
                }))
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        manager.disconnect(websocket)
