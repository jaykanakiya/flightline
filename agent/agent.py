"""Minimal example agent: an agent_step, an llm_call, and a tool_call, sent
as real spans to the ingest service.

The LLM step is mocked by default so this runs with no API key - set
OPENAI_API_KEY to call a real model instead. This agent is just a
demonstration that real traces look like the generator's synthetic ones;
the generator is what actually drives the benchmark.
"""
import hashlib
import os
import random
import time
import uuid
from datetime import datetime, timezone

import requests

from tools import TOOLS

INGEST_URL = os.environ.get("INGEST_URL", "http://localhost:8080/v1/spans")
MODEL = os.environ.get("AGENT_MODEL", "gpt-4o-mini")


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def input_hash(*parts) -> str:
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:12]


def call_llm(prompt: str) -> str:
    """Mocked by default. Set OPENAI_API_KEY to call a real model instead."""
    if os.environ.get("OPENAI_API_KEY"):
        from openai import OpenAI

        client = OpenAI()
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content

    time.sleep(random.uniform(0.1, 0.4))
    return random.choice(list(TOOLS.keys()))


def run_agent(task: str, run_id: str | None = None) -> dict:
    run_id = run_id or str(uuid.uuid4())
    parent_span_id = ""
    spans = []

    def emit(step_type: str, start: float, end: float, **fields) -> None:
        nonlocal parent_span_id
        span_id = str(uuid.uuid4())
        spans.append({
            "trace_id": run_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "agent_name": "python-example-agent",
            "run_id": run_id,
            "step_type": step_type,
            "status": "ok",
            "start_time": iso(start),
            "end_time": iso(end),
            "latency_ms": int((end - start) * 1000),
            **fields,
        })
        parent_span_id = span_id

    t = time.time()
    emit("agent_step", t, t)

    t0 = time.time()
    tool_name = call_llm(f"Which tool should I use for: {task}?")
    t1 = time.time()
    emit(
        "llm_call", t0, t1,
        model=MODEL,
        input_tokens=random.randint(50, 200),
        output_tokens=random.randint(5, 30),
        cost_usd=round(random.uniform(0.0001, 0.002), 6),
    )

    t0 = time.time()
    try:
        if tool_name == "add":
            TOOLS["add"](random.randint(1, 100), random.randint(1, 100))
        elif tool_name == "word_count":
            TOOLS["word_count"](task)
        else:
            TOOLS["get_time"]()
        status, error_message = "ok", ""
    except Exception as e:  # example agent - keep failure handling simple
        status, error_message = "error", str(e)
    t1 = time.time()
    emit(
        "tool_call", t0, t1,
        tool_name=tool_name,
        status=status,
        error_message=error_message,
        input_hash=input_hash(tool_name, task),
    )

    resp = requests.post(INGEST_URL, json={"spans": spans}, timeout=5)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    tasks = [
        "What time is it?",
        "Count the words in this sentence",
        "Add two numbers together",
    ]
    for task in tasks:
        result = run_agent(task)
        print(f"sent run for task={task!r}: {result}")
