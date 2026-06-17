Prerequisites
Ollama: Must be installed and running locally (default model: huihui_ai/granite4.1-abliterated:8b).

System Tools: xdotool (required for active window tracking on Linux).

Python Packages: pyzmq, psutil, and httpx.

Components
event_sensor.py (The Monitor) Listens for OS events—specifically active window changes and newly launched processes. It classifies these activities (e.g., browser tabs, text files, videos) and publishes them as JSON payloads to a ZeroMQ socket.

lab_agent.py (The Assistant) Subscribes to the ZeroMQ event stream. At startup, it prompts you for your current focus goal. It continually feeds recent desktop events to the local Ollama LLM, which replies with concise affirmations if you are on task, or short nudges if you are distracted. It also accepts manual chat inputs via the terminal so you can interact with the agent directly.

i3-config-kiosk (Ubunt i3wm X11 setup file) config file to configure UX
cp i3-config-kiosk ~/.config/i3/config