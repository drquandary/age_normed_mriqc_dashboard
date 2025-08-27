from fastapi import FastAPI, Request, WebSocket, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
from .routes import router, add_security_headers
from .error_handling import setup_logging, error_handler_middleware
from .security import data_retention_manager, security_auditor

# Get the directory containing this file
BASE_DIR = Path(__file__).resolve().parent

# Setup logging
setup_logging()

app = FastAPI(
    title="Age-Normed MRIQC Dashboard",
    description="Secure MRIQC quality assessment with age-specific normative thresholds",
    version="1.0.0"
)

# Security middleware
@app.middleware("http")
async def security_middleware(request: Request, call_next):
    """Add security headers and logging to all responses."""
    # Log request for security audit
    security_auditor.log_data_access(
        resource=str(request.url.path),
        client_ip=request.client.host if request.client else "unknown",
        user_agent=request.headers.get("User-Agent", "unknown")
    )
    
    response = await call_next(request)
    
    # Add security headers
    response = add_security_headers(response)
    
    return response

# Add CORS middleware with security considerations
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8000"],  # Restrict origins
    allow_credentials=False,  # Don't allow credentials for security
    allow_methods=["GET", "POST"],  # Restrict methods
    allow_headers=["*"],
)

# Add error handling middleware
app.middleware("http")(error_handler_middleware)

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
        from .error_handling import error_handler
        import logging
        
        logger = logging.getLogger(__name__)
        error_response = error_handler.handle_system_error(
            component="websocket",
            message="WebSocket connection error",
            exception=e
        )
        logger.error(f"WebSocket error: {error_response.message}", extra={
            'error_id': error_response.error_id,
            'error_code': error_response.error_code
        })
    finally:
        manager.disconnect(websocket)

# Security service lifecycle events
@app.on_event("startup")
async def startup_event():
    """Initialize security services on startup."""
    # Start data retention cleanup service
    data_retention_manager.start_cleanup_service()
    
    # Log application startup
    security_auditor.log_security_event(
        'application_startup',
        {'version': '1.0.0', 'security_enabled': True},
        'LOW'
    )


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up security services on shutdown."""
    # Stop data retention cleanup service
    data_retention_manager.stop_cleanup_service()
    
    # Log application shutdown
    security_auditor.log_security_event(
        'application_shutdown',
        {'graceful_shutdown': True},
        'LOW'
    )


# Health check endpoint with security status
@app.get('/health')
async def health_check():
    """Health check endpoint with security status."""
    return {
        'status': 'healthy',
        'security_enabled': True,
        'data_retention_active': data_retention_manager.running,
        'audit_logging_enabled': security_auditor.enabled
    }