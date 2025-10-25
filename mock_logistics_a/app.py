from fastapi import FastAPI
from pydantic import BaseModel
import json
from typing import List

app = FastAPI(title="Mock Partner A", root_path="/mock-a")

with open("/srv/data.json") as f:
    data = json.load(f)

class LogisticsAResponse(BaseModel):
    deliveryId: str
    supplier: str
    timestamp: str
    status: str
    signedBy: str

@app.post("/api/logistics-a", response_model=List[LogisticsAResponse])
async def logistics_a():
    return data

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
