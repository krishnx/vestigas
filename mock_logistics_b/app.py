
from fastapi import FastAPI
from pydantic import BaseModel
import json
from typing import List

app = FastAPI(title="Mock Partner B", root_path="/mock-b")

with open("/srv/data.json") as f:
    data = json.load(f)

class Receiver(BaseModel):
    name: str
    signed: bool

class LogisticsBResponse(BaseModel):
    id: str
    provider: str
    deliveredAt: str
    statusCode: str
    receiver: Receiver

@app.post("/api/logistics-b", response_model=List[LogisticsBResponse])
async def logistics_b():
    return data

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
