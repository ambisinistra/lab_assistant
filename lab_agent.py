"""
focus_agent.py — local Ollama agent that helps you stay focused.

Usage:
    python focus_agent.py

The agent:
  1. Prompts for your goal interactively at startup
  2. Subscribes to ZeroMQ events from agent_monitor.py
  3. Every 1.5 seconds — if events arrived — asks Ollama to react
  4. Also accepts manual messages typed anytime in the same terminal
  5. Streams all responses token-by-token to stdout
"""

import asyncio
import json
import sys
from collections import deque
from datetime import datetime, timezone
from typing import Any

import httpx
import zmq
import zmq.asyncio

# ── Config ────────────────────────────────────────────────────────────────────

ZMQ_ADDRESS   = "tcp://127.0.0.1:5555"
OLLAMA_URL    = "http://localhost:11434/api/chat"
OLLAMA_MODEL  = "huihui_ai/granite4.1-abliterated:8b"
POLL_INTERVAL = 1.5
MAX_EVENTS    = 20
STREAM        = True

SYSTEM_TEMPLATE = """\
You are a focus assistant running on the user's desktop.
The user's current main goal is:

  {goal}

Your only job is to help the user stay focused on that goal.
You receive either:
  (a) a short list of recent desktop events (files opened, browser tabs, \
processes launched), or
  (b) a direct message from the user.

For desktop events — analyse briefly and do ONE of the following:
  • If the events are clearly related to the goal → affirm in one sentence.
  • If the events are ambiguous → ask one short clarifying question.
  • If the events are clearly off-topic (distraction) → give a calm, \
    non-judgmental one-sentence nudge back to the goal.

For direct messages — respond helpfully and concisely in the context of the goal.

Rules:
  - Never repeat the event list back to the user.
  - Be concise: 1-3 sentences maximum.
  - Do not lecture or moralize.
  - Current UTC time: {time}
"""

# ── ZeroMQ subscriber ─────────────────────────────────────────────────────────

class EventQueue:
    def __init__(self, address: str, maxlen: int = MAX_EVENTS):
        self._ctx    = zmq.asyncio.Context()
        self._socket = self._ctx.socket(zmq.SUB)
        self._socket.connect(address)
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "event")
        self._buf: deque[dict] = deque(maxlen=maxlen)
        print(f"[ZMQ] SUB connected to {address}")

    async def drain(self) -> None:
        while True:
            try:
                parts = await self._socket.recv_multipart(flags=zmq.NOBLOCK)
                if len(parts) == 2:
                    self._buf.append(json.loads(parts[1].decode()))
            except zmq.Again:
                break

    def flush(self) -> list[dict]:
        events = list(self._buf)
        self._buf.clear()
        return events

    def close(self) -> None:
        self._socket.close()
        self._ctx.term()


# ── Ollama streaming call ─────────────────────────────────────────────────────

def format_events(events: list[dict]) -> str:
    lines = []
    for e in events:
        meta  = e.get("meta", {})
        label = meta.get("title") or meta.get("path") or meta.get("process") or ""
        lines.append(
            f"  [{e['time']}] {e['kind']} → {e['action']}"
            + (f"  ({label})" if label else "")
        )
    return "\n".join(lines)


# Serialise Ollama calls so manual + event responses don't interleave
_ollama_lock = asyncio.Lock()

async def ask_ollama(
    system: str,
    user_msg: str,
    client: httpx.AsyncClient,
    label: str = "Agent",
) -> None:
    payload = {
        "model":    OLLAMA_MODEL,
        "stream":   STREAM,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user_msg},
        ],
    }

    async with _ollama_lock:
        print(f"\n\033[36m┌─ {label} {'─' * max(0, 40 - len(label))}\033[0m")
        print("\033[36m│\033[0m ", end="", flush=True)

        try:
            async with client.stream("POST", OLLAMA_URL, json=payload, timeout=60) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line:
                        continue
                    chunk: dict[str, Any] = json.loads(line)
                    token: str = chunk.get("message", {}).get("content", "")
                    for ch in token:
                        if ch == "\n":
                            print(f"\n\033[36m│\033[0m ", end="", flush=True)
                        else:
                            print(ch, end="", flush=True)
                    if chunk.get("done"):
                        break
        except httpx.ConnectError:
            print(f"\n[ERROR] Cannot reach Ollama at {OLLAMA_URL}. Is it running?",
                  file=sys.stderr)
        except Exception as exc:
            print(f"\n[ERROR] Ollama call failed: {exc}", file=sys.stderr)

        print(f"\n\033[36m└{'─' * 42}\033[0m\n", flush=True)


# ── Event loop task ───────────────────────────────────────────────────────────

async def event_loop(system: str, client: httpx.AsyncClient) -> None:
    queue = EventQueue(ZMQ_ADDRESS)
    try:
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            await queue.drain()
            events = queue.flush()
            if not events:
                continue
            user_msg = (
                f"Here are the desktop events from the last {POLL_INTERVAL:.0f} seconds:\n"
                + format_events(events)
            )
            await ask_ollama(system, user_msg, client, label="Agent · events")
    except asyncio.CancelledError:
        pass
    finally:
        queue.close()


# ── Stdin reader task ─────────────────────────────────────────────────────────

async def stdin_reader(system: str, client: httpx.AsyncClient) -> None:
    """Read lines from stdin without blocking the event loop."""
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    try:
        while True:
            line_bytes = await reader.readline()
            if not line_bytes:          # EOF
                break
            text = line_bytes.decode().rstrip("\n").strip()
            if not text:
                continue
            await ask_ollama(system, text, client, label="Agent · you")
    except asyncio.CancelledError:
        pass


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    try:
        goal = input("What is your main goal right now? › ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nNo goal provided. Exiting.")
        sys.exit(0)

    if not goal:
        print("Goal cannot be empty.")
        sys.exit(1)

    system = SYSTEM_TEMPLATE.format(
        goal=goal,
        time=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )

    print(f"\n\033[32m● Focus agent started\033[0m")
    print(f"\033[32m  Goal  : {goal}\033[0m")
    print(f"\033[32m  Model : {OLLAMA_MODEL}  |  poll: {POLL_INTERVAL}s\033[0m")
    print(f"\033[32m  Type a message anytime to talk to the agent.\033[0m\n")

    async with httpx.AsyncClient() as client:
        tasks = [
            asyncio.create_task(event_loop(system, client)),
            asyncio.create_task(stdin_reader(system, client)),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\033[33mFocus agent stopped.\033[0m")
        sys.exit(0)