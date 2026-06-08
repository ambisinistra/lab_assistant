"""
focus_agent.py — local Ollama agent that helps you stay focused.

Usage:
    python focus_agent.py "Implement ZeroMQ event bus for the lab assistant"
    python focus_agent.py "Write chapter 3 of my novel"

The agent:
  1. Puts your goal into the system prompt
  2. Subscribes to ZeroMQ events from agent_monitor.py
  3. Every 3 seconds — if events arrived — asks Ollama to react
  4. Streams the response token-by-token to stdout
"""

import argparse
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

ZMQ_ADDRESS   = "tcp://127.0.0.1:5555"   # must match agent_monitor.py
OLLAMA_URL    = "http://localhost:11434/api/chat"
OLLAMA_MODEL  = "huihui_ai/granite4.1-abliterated:8b"            # change to any model you have locally
POLL_INTERVAL = 1.5                       # seconds between Ollama calls
MAX_EVENTS    = 20                        # max events per batch sent to LLM
STREAM        = True                      # stream tokens as they arrive

SYSTEM_TEMPLATE = """\
You are a focus assistant running on the user's desktop.
The user's current main goal is:

  {goal}

Your only job is to help the user stay focused on that goal.
You receive a short list of recent desktop events (files opened, browser tabs, \
processes launched). Analyse them briefly and do ONE of the following:

  • If the events are clearly related to the goal → affirm in one sentence.
  • If the events are ambiguous → ask one short clarifying question.
  • If the events are clearly off-topic (distraction) → give a calm, \
    non-judgmental one-sentence nudge back to the goal.

Rules:
  - Never repeat the event list back to the user.
  - Be concise: 1-3 sentences maximum.
  - Do not lecture or moralize.
  - Current UTC time: {time}
"""

# ── ZeroMQ subscriber (non-blocking, asyncio) ─────────────────────────────────

class EventQueue:
    """Drains ZMQ SUB socket into an in-process deque."""

    def __init__(self, address: str, maxlen: int = MAX_EVENTS):
        self._ctx    = zmq.asyncio.Context()
        self._socket = self._ctx.socket(zmq.SUB)
        self._socket.connect(address)
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "event")
        self._socket.setsockopt(zmq.RCVTIMEO, 0)   # non-blocking
        self._buf: deque[dict] = deque(maxlen=maxlen)
        print(f"[ZMQ] SUB connected to {address}")

    async def drain(self) -> None:
        """Pull all currently available messages into the buffer (non-blocking)."""
        while True:
            try:
                parts = await self._socket.recv_multipart(flags=zmq.NOBLOCK)
                # parts = [topic_bytes, payload_bytes]
                if len(parts) == 2:
                    event = json.loads(parts[1].decode())
                    self._buf.append(event)
            except zmq.Again:
                break   # no more messages right now

    def flush(self) -> list[dict]:
        """Return and clear the buffer."""
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
        meta = e.get("meta", {})
        label = meta.get("title") or meta.get("path") or meta.get("process") or ""
        lines.append(f"  [{e['time']}] {e['kind']} → {e['action']}"
                     + (f"  ({label})" if label else ""))
    return "\n".join(lines)


async def ask_ollama(system: str, user_msg: str, client: httpx.AsyncClient) -> None:
    """Stream Ollama response to stdout."""
    payload = {
        "model":    OLLAMA_MODEL,
        "stream":   STREAM,
        "messages": [
            {"role": "system",  "content": system},
            {"role": "user",    "content": user_msg},
        ],
    }

    print("\n\033[36m┌─ Agent ────────────────────────────────\033[0m")
    print("\033[36m│\033[0m ", end="", flush=True)

    try:
        async with client.stream("POST", OLLAMA_URL, json=payload, timeout=60) as resp:
            resp.raise_for_status()
            col = 2  # track column for soft line-wrapping inside the box
            async for line in resp.aiter_lines():
                if not line:
                    continue
                chunk: dict[str, Any] = json.loads(line)
                token: str = chunk.get("message", {}).get("content", "")
                if token:
                    # prefix each newline with the box border
                    for ch in token:
                        if ch == "\n":
                            print(f"\n\033[36m│\033[0m ", end="", flush=True)
                            col = 2
                        else:
                            print(ch, end="", flush=True)
                            col += 1
                if chunk.get("done"):
                    break
    except httpx.ConnectError:
        print(f"\n[ERROR] Cannot reach Ollama at {OLLAMA_URL}. Is it running?",
              file=sys.stderr)
    except Exception as exc:
        print(f"\n[ERROR] Ollama call failed: {exc}", file=sys.stderr)

    print(f"\n\033[36m└────────────────────────────────────────\033[0m\n",
          flush=True)


# ── Main loop ─────────────────────────────────────────────────────────────────

async def agent_loop(goal: str) -> None:
    system_prompt = SYSTEM_TEMPLATE.format(
        goal=goal,
        time=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )

    queue  = EventQueue(ZMQ_ADDRESS)
    client = httpx.AsyncClient()

    print(f"\n\033[32m● Focus agent started\033[0m")
    print(f"\033[32m  Goal: {goal}\033[0m")
    print(f"\033[32m  Model: {OLLAMA_MODEL}  |  poll: {POLL_INTERVAL}s\033[0m\n")

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

            await ask_ollama(system_prompt, user_msg, client)

    except asyncio.CancelledError:
        pass
    finally:
        queue.close()
        await client.aclose()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ollama focus agent — reacts to your desktop events."
    )
    parser.add_argument(
        "goal",
        nargs="?",
        default=None,
        help="Your current main task, e.g. 'Write the ZeroMQ event bus'",
    )
    args = parser.parse_args()

    goal = args.goal
    if not goal:
        try:
            goal = input("What is your main goal right now? › ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nNo goal provided. Exiting.")
            sys.exit(0)

    if not goal:
        print("Goal cannot be empty.")
        sys.exit(1)

    try:
        asyncio.run(agent_loop(goal))
    except KeyboardInterrupt:
        print("\n\033[33mFocus agent stopped.\033[0m")
        sys.exit(0)


if __name__ == "__main__":
    main()