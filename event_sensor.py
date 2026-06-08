"""
agent_monitor.py — publishes typed file/tab events to ZeroMQ PUB socket.

Topic:  b"event"
Payload: JSON — {"kind": ..., "action": ..., "time": ..., "meta": ...}

Subscriber example:
    import zmq, json
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect("tcp://localhost:5555")
    sub.setsockopt_string(zmq.SUBSCRIBE, "event")
    while True:
        topic, raw = sub.recv_multipart()
        print(json.loads(raw))
"""

import asyncio
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, TypedDict

import psutil
import zmq
import zmq.asyncio

# ── Config ────────────────────────────────────────────────────────────────────

ZMQ_ADDRESS   = "tcp://127.0.0.1:5555"   # change to "ipc:///tmp/agent.ipc" for UNIX socket
POLL_WINDOW_S = 2    # active-window poll interval
POLL_PROC_S   = 3    # new-process poll interval

# Extensions → kind
EXT_MAP: dict[str, str] = {
    # text
    ".txt": "text_file", ".md": "text_file", ".rst": "text_file",
    ".py": "text_file",  ".js": "text_file", ".ts": "text_file",
    ".json": "text_file",".yaml": "text_file",".yml": "text_file",
    ".csv": "text_file", ".log": "text_file", ".xml": "text_file",
    ".html": "text_file",".css": "text_file", ".sh": "text_file",
    # image
    ".jpg": "image_file", ".jpeg": "image_file", ".png": "image_file",
    ".gif": "image_file", ".bmp": "image_file",  ".svg": "image_file",
    ".webp": "image_file",".tiff": "image_file", ".ico": "image_file",
    # video
    ".mp4": "video_file", ".mkv": "video_file", ".avi": "video_file",
    ".mov": "video_file", ".webm": "video_file", ".flv": "video_file",
    ".wmv": "video_file", ".m4v": "video_file",
}

# Process names considered browsers
BROWSER_NAMES = {
    "firefox", "firefox-bin", "chromium", "chromium-browser",
    "google-chrome", "chrome", "brave-browser", "opera", "vivaldi-bin",
}

# ── Types ─────────────────────────────────────────────────────────────────────

Kind   = Literal["browser_tab", "text_file", "image_file", "video_file"]
Action = Literal["open", "closed"]

class Event(TypedDict):
    kind:   Kind
    action: Action
    time:   str          # ISO-8601 UTC
    meta:   dict         # window title, pid, path, etc.


# ── Publisher ─────────────────────────────────────────────────────────────────

class ZMQPublisher:
    def __init__(self, address: str):
        self._ctx    = zmq.asyncio.Context()
        self._socket = self._ctx.socket(zmq.PUB)
        self._socket.bind(address)
        print(f"[ZMQ] PUB bound to {address}")

    async def send(self, event: Event) -> None:
        payload = json.dumps(event, ensure_ascii=False).encode()
        await self._socket.send_multipart([b"event", payload])
        print(f"[PUB] {event['kind']:12s} | {event['action']:6s} | {event['meta']}")

    def close(self) -> None:
        self._socket.close()
        self._ctx.term()


# ── Helpers ───────────────────────────────────────────────────────────────────

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def classify_window(title: str) -> Kind | None:
    """
    Return Kind from window title, or None if unrecognised.
    Priority: URL pattern → file extension → known browser titles.
    """
    low = title.lower()

    # Browser heuristic: title ends with " — Firefox", " - Chromium", etc.
    browser_suffixes = (
        "firefox", "chromium", "chrome", "brave", "opera", "vivaldi", "epiphany"
    )
    if any(low.endswith(s) or f" — {s}" in low or f" - {s}" in low
           for s in browser_suffixes):
        return "browser_tab"

    # File extension in title (e.g. Evince, eog, VLC, gedit, VSCode)
    for ext, kind in EXT_MAP.items():
        if low.endswith(ext) or f"{ext} " in low or f"{ext})" in low:
            return kind  # type: ignore[return-value]

    return None


def is_browser_proc(name: str) -> bool:
    return name.lower().rstrip("-bin") in BROWSER_NAMES or name.lower() in BROWSER_NAMES


# ── Tasks ─────────────────────────────────────────────────────────────────────

async def track_active_window(pub: ZMQPublisher) -> None:
    """Poll xdotool; emit event when focused window changes."""
    prev_title = ""
    prev_kind: Kind | None = None

    while True:
        try:
            proc = await asyncio.create_subprocess_shell(
                "xdotool getactivewindow getwindowname 2>/dev/null",
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            stdout, _ = await proc.communicate()
            title = stdout.decode().strip()

            if not title or title == prev_title:
                await asyncio.sleep(POLL_WINDOW_S)
                continue

            kind = classify_window(title)

            # Emit "closed" for previous window if it had a kind
            if prev_kind and prev_kind != kind:
                await pub.send(Event(
                    kind=prev_kind,
                    action="closed",
                    time=utc_now(),
                    meta={"title": prev_title},
                ))

            # Emit "open" for new window if it has a kind
            if kind:
                await pub.send(Event(
                    kind=kind,
                    action="open",
                    time=utc_now(),
                    meta={"title": title},
                ))

            prev_title = title
            prev_kind  = kind

        except Exception as exc:
            print(f"[WARN] track_active_window: {exc}", file=sys.stderr)

        await asyncio.sleep(POLL_WINDOW_S)


async def track_new_processes(pub: ZMQPublisher) -> None:
    """
    Detect newly spawned processes.
    Emits browser_tab open when a browser starts;
    emits *_file open when a file-opener process appears with a known extension.
    """
    seen_pids: set[int] = set(psutil.pids())

    while True:
        await asyncio.sleep(POLL_PROC_S)
        try:
            current_pids = set(psutil.pids())
            new_pids     = current_pids - seen_pids
            seen_pids    = current_pids

            for pid in new_pids:
                try:
                    p    = psutil.Process(pid)
                    name = p.name()
                    cmdline: list[str] = p.cmdline()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

                # Browser started
                if is_browser_proc(name):
                    await pub.send(Event(
                        kind="browser_tab",
                        action="open",
                        time=utc_now(),
                        meta={"process": name, "pid": pid},
                    ))
                    continue

                # Check cmdline args for files with known extensions
                for arg in cmdline[1:]:
                    path = Path(arg)
                    kind = EXT_MAP.get(path.suffix.lower())
                    if kind and path.exists():
                        await pub.send(Event(
                            kind=kind,          # type: ignore[arg-type]
                            action="open",
                            time=utc_now(),
                            meta={"path": str(path), "process": name, "pid": pid},
                        ))
                        break   # one event per process launch

        except Exception as exc:
            print(f"[WARN] track_new_processes: {exc}", file=sys.stderr)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    pub = ZMQPublisher(ZMQ_ADDRESS)
    print("Agent started. Ctrl-C to stop.\n")
    try:
        await asyncio.gather(
            track_active_window(pub),
            track_new_processes(pub),
        )
    finally:
        pub.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAgent stopped.")
        sys.exit(0)