from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from langgraph.graph import StateGraph
from main import compiled
import uvicorn

app = FastAPI()

class GroupInput(BaseModel):
    group_id: int
    file_path: str

class InputPayload(BaseModel):
    groups: List[GroupInput]

@app.post("/run-langgraph")
async def run_batch_workflow(payload: InputPayload):
    results = []
    for group in payload.groups:
        try:
            result = compiled.invoke(group.dict())
            results.append({
                "group_id": group.group_id,
                "status": "success",
                "result": result
            })
        except Exception as e:
            results.append({
                "group_id": group.group_id,
                "status": "error",
                "message": str(e)
            })
    return {"overall_status": "done", "details": results}

if __name__ == "__main__":
    uvicorn.run("API:app", host="0.0.0.0", port=8000, reload=True)
