from fastapi import FastAPI
from .routes import router


app = FastAPI(title="Age-Normed MRIQC Dashboard")

# Include API routes under /api
app.include_router(router, prefix='/api')


@app.get('/')
async def root():
    """Root endpoint providing basic project information."""
    return {'detail': 'Age-Normed MRIQC Dashboard API'}
