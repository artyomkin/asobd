import asyncio
import threading
from datetime import datetime
from typing import List

from fastapi import FastAPI
from prefect.client import get_client

app = FastAPI()

async def flush_batch(batch):
    async with get_client() as client:
        await client.create_flow_run_from_deployment(
            deployment_id="74a505c5-aefb-49b6-adf5-69f5bc3ebb86",
            parameters={"events": batch}
        )
    batch.clear()
    return {"status": "queued"}

@app.post("/event")
async def receive_event(events: List[dict]):
    print(len(events))
    asyncio.create_task(flush_batch(events))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)