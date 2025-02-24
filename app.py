from typing import Optional
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import requests
from tinydb import TinyDB, Query

app = FastAPI()

node_name = "Pi"

db_path = f"./db_{node_name}.json"
node_db = TinyDB(db_path)


class Rule(BaseModel):
    chain_id: int
    next_hop_id: Optional[str] = None
    next_hop_base_url: Optional[str] = None


class PingRequest(BaseModel):
    chain_id: int
    dst_node: str


@app.get("/hello")
def hello():
    return {"status": "success", "message": f"Hello from {node_name}\n"}


@app.post("/apply_rule")
def apply_rule(rule: Rule):
    simulate_disk_io("rule")

    q = Query()
    existing = node_db.get(q.chain_id == rule.chain_id)
    if existing:
        node_db.update(
            {
                "next_node_id": rule.next_hop_id,
                "next_hop_base_url": rule.next_hop_base_url,
            },
            q.chain_id == rule.chain_id,
        )
    else:
        node_db.insert(
            {
                "chain_id": rule.chain_id,
                "next_node_id": rule.next_hop_id,
                "next_hop_base_url": rule.next_hop_base_url,
                "current_node_id": node_name,
            }
        )
    return {"status": "success", "rule_applied": rule.dict()}


@app.post("/ping")
def ping(req: PingRequest):
    if node_name == req.dst_node:
        print("Destination Reached for", req)
        return {"status": "reached", "node": node_name}

    q = Query()
    chain_rule = node_db.get(q.chain_id == req.chain_id)
    if not chain_rule:
        raise HTTPException(status_code=404, detail="No chain rule found on this node.")

    next_node_id = chain_rule.get("next_node_id")
    next_hop_base_url = chain_rule.get("next_hop_base_url")

    simulate_disk_io("ping")

    print(f"Hopped on node {node_name}, forwarding to node {next_node_id}")
    if not next_node_id:
        raise HTTPException(
            status_code=500, detail="No next hop found and destination not reached."
        )

    next_node_url = f"{next_hop_base_url}/ping"
    try:
        response = requests.post(
            next_node_url,
            json={"chain_id": req.chain_id, "dst_node": req.dst_node},
            timeout=5,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to forward ping: {str(e)}")


def simulate_disk_io(seed):
    filename = f"/tmp/large_testfile_{seed}"
    with open(filename, "wb") as f:
        f.write(os.urandom(1024 * 1024 * 10))
    os.remove(filename)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=50100, log_level="debug")
