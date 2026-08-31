import asyncio
import io
import json
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import anyio
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

# ---------------------------------------------------------------------------
# App & startup
# ---------------------------------------------------------------------------

app = FastAPI()
questions: dict[int, dict] = {}


@app.on_event("startup")
async def startup() -> None:
    Path("results").mkdir(exist_ok=True)
    quiz_dir = Path("quiz")
    for f in sorted(quiz_dir.glob("*.json"), key=lambda p: int(p.stem)):
        questions[int(f.stem)] = json.loads(f.read_text())


# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------


@dataclass
class QuizState:
    current_question: int = 0
    question_started_at: float = 0.0
    listeners: list = field(default_factory=list)


state = QuizState()
state_lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def format_state_event(q_num: int) -> str:
    if q_num == 0:
        payload = {"question": 0, "status": "waiting"}
    elif q_num == -1:
        payload = {"question": -1, "status": "closed"}
    else:
        q = questions.get(q_num, {})
        payload = {
            "question": q_num,
            "status": "active",
            "question_text": q.get("question", ""),
            "choices": q.get("choices", []),
        }
    return f"event: state\ndata: {json.dumps(payload)}\n\n"


async def broadcast(event_str: str) -> None:
    async with state_lock:
        for q in list(state.listeners):
            await q.put(event_str)


async def save_result(name: str, access_time: str, answers: dict) -> None:
    safe = "".join(c for c in name if c.isalnum() or c in "-_ ")[:64].strip()
    if not safe:
        safe = "unknown"
    path = Path("results") / f"{safe}.txt"
    lines = [f"access_time,{access_time}\n"]
    for key in sorted(answers.keys()):
        a = answers[key]
        lines.append(f"{key},{a.get('answer','')},{a.get('time', 0)}\n")
    async with await anyio.open_file(path, "w") as f:
        await f.writelines(lines)


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

PARTICIPANT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Quiz</title>
<style>
  body { font-family: sans-serif; max-width: 600px; margin: 60px auto; padding: 0 16px; }
  h1 { font-size: 1.4rem; }
  .hidden { display: none; }
  .choice { display: block; margin: 8px 0; font-size: 1rem; cursor: pointer; }
  button { margin-top: 16px; padding: 10px 24px; font-size: 1rem; cursor: pointer; }
  #status { color: #555; font-style: italic; }
  #thankyou { font-size: 1.6rem; font-weight: bold; }
</style>
</head>
<body>

<div id="name-section">
  <h1>Welcome to the Quiz</h1>
  <label>Your name: <input id="name-input" type="text" placeholder="Enter your name"></label>
  <button onclick="saveName()">Enter</button>
</div>

<div id="waiting-section" class="hidden">
  <p id="status">Waiting for the quiz to start...</p>
</div>

<div id="question-section" class="hidden">
  <h1 id="q-text"></h1>
  <form id="answer-form"></form>
  <button id="submit-btn" onclick="submitAnswer()">Submit</button>
</div>

<div id="thankyou" class="hidden">Thank you!</div>

<script>
// --- Cookie utilities ---
function setCookie(name, value) {
  document.cookie = name + '=' + encodeURIComponent(value) + '; path=/; SameSite=Lax';
}
function getCookie(name) {
  const m = document.cookie.match('(?:^|; )' + name + '=([^;]*)');
  return m ? decodeURIComponent(m[1]) : null;
}
function readAnswers() {
  const raw = getCookie('answers');
  return raw ? JSON.parse(raw) : {};
}
function writeAnswers(obj) {
  setCookie('answers', JSON.stringify(obj));
}

// --- Init ---
let currentQuestion = 0;

window.addEventListener('DOMContentLoaded', () => {
  const name = getCookie('name');
  if (name) {
    show('waiting-section');
    hide('name-section');
  }
  connectSSE();
});

function saveName() {
  const name = document.getElementById('name-input').value.trim();
  if (!name) return;
  setCookie('name', name);
  setCookie('access_time', new Date().toISOString());
  hide('name-section');
  show('waiting-section');
}

// --- SSE ---
function connectSSE() {
  const source = new EventSource('/events');
  source.addEventListener('state', e => handleState(JSON.parse(e.data)));
  source.onerror = () => setTimeout(connectSSE, 3000);
}

function handleState(data) {
  if (data.status === 'waiting') {
    hide('question-section');
    hide('thankyou');
    show('waiting-section');
  } else if (data.status === 'active') {
    currentQuestion = data.question;
    renderQuestion(data);
    hide('waiting-section');
    hide('thankyou');
    show('question-section');
  } else if (data.status === 'closed') {
    hide('question-section');
    hide('waiting-section');
    show('thankyou');
    sendFinalSubmit();
  }
}

function renderQuestion(data) {
  document.getElementById('q-text').textContent =
    'Q' + data.question + '. ' + data.question_text;
  const form = document.getElementById('answer-form');
  form.innerHTML = '';
  data.choices.forEach(choice => {
    const label = document.createElement('label');
    label.className = 'choice';
    const radio = document.createElement('input');
    radio.type = 'radio';
    radio.name = 'choice';
    radio.value = choice;
    label.appendChild(radio);
    label.appendChild(document.createTextNode(' ' + choice));
    form.appendChild(label);
  });
  const btn = document.getElementById('submit-btn');
  btn.disabled = false;
}

// --- Submit answer ---
async function submitAnswer() {
  const selected = document.querySelector('#answer-form input[name=choice]:checked');
  if (!selected) { alert('Please select an answer.'); return; }
  const btn = document.getElementById('submit-btn');
  btn.disabled = true;

  const resp = await fetch('/answer', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({question: currentQuestion, answer: selected.value})
  });
  const {time_ms} = await resp.json();

  const answers = readAnswers();
  const key = 'q' + currentQuestion;
  if (!answers[key]) {
    answers[key] = {answer: selected.value, time: time_ms};
    writeAnswers(answers);
  }
}

// --- Final submit ---
async function sendFinalSubmit() {
  await fetch('/submit', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      name: getCookie('name') || 'unknown',
      access_time: getCookie('access_time') || '',
      answers: readAnswers()
    })
  });
}

function show(id) { document.getElementById(id).classList.remove('hidden'); }
function hide(id) { document.getElementById(id).classList.add('hidden'); }
</script>
</body>
</html>
"""

ADMIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Quiz Admin</title>
<style>
  body { font-family: sans-serif; max-width: 500px; margin: 60px auto; padding: 0 16px; }
  button { padding: 12px 28px; font-size: 1rem; margin: 8px 8px 8px 0; cursor: pointer; }
  #status { margin-top: 20px; padding: 12px; background: #f4f4f4; border-radius: 4px;
            font-family: monospace; white-space: pre-wrap; }
</style>
</head>
<body>
<h1>Quiz Admin</h1>
<button onclick="adminAction('/admin/start')">Start</button>
<button onclick="adminAction('/admin/next')">Next</button>
<button onclick="adminAction('/admin/close')">Close</button>
<a href="/admin/results" download><button type="button">Download Results</button></a>
<div id="status">—</div>
<script>
async function adminAction(path) {
  const resp = await fetch(path, {method: 'POST'});
  const data = await resp.json();
  document.getElementById('status').textContent = JSON.stringify(data, null, 2);
}
</script>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def participant_page() -> HTMLResponse:
    return HTMLResponse(PARTICIPANT_HTML)


@app.get("/admin", response_class=HTMLResponse)
async def admin_page() -> HTMLResponse:
    return HTMLResponse(ADMIN_HTML)


@app.get("/events")
async def sse_events() -> StreamingResponse:
    queue: asyncio.Queue = asyncio.Queue()

    async def generator():
        async with state_lock:
            state.listeners.append(queue)
            current = format_state_event(state.current_question)
        try:
            yield current
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=20.0)
                    yield msg
                except asyncio.TimeoutError:
                    yield "event: ping\ndata: {}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            async with state_lock:
                try:
                    state.listeners.remove(queue)
                except ValueError:
                    pass

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return StreamingResponse(generator(), media_type="text/event-stream", headers=headers)


@app.post("/answer")
async def answer(request: Request) -> JSONResponse:
    body = await request.json()
    async with state_lock:
        elapsed = time.monotonic() - state.question_started_at
    time_ms = round(elapsed * 1000)
    return JSONResponse({"time_ms": time_ms})


@app.post("/submit")
async def submit(request: Request) -> JSONResponse:
    body = await request.json()
    name = str(body.get("name", "unknown"))
    access_time = str(body.get("access_time", ""))
    answers = body.get("answers", {})
    await save_result(name, access_time, answers)
    return JSONResponse({"ok": True})


@app.post("/admin/start")
async def admin_start() -> JSONResponse:
    if not questions:
        return JSONResponse({"ok": False, "error": "no quiz files found"})
    async with state_lock:
        state.current_question = 1
        state.question_started_at = time.monotonic()
        event = format_state_event(1)
    await broadcast(event)
    return JSONResponse({"ok": True, "question": 1})


@app.post("/admin/next")
async def admin_next() -> JSONResponse:
    max_q = max(questions.keys()) if questions else 0
    async with state_lock:
        if state.current_question >= max_q:
            return JSONResponse({"ok": False, "error": "already at last question"})
        state.current_question += 1
        state.question_started_at = time.monotonic()
        q = state.current_question
        event = format_state_event(q)
    await broadcast(event)
    return JSONResponse({"ok": True, "question": q})


@app.get("/admin/results")
async def admin_results() -> StreamingResponse:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(Path("results").glob("*.txt")):
            zf.write(f, f.name)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=results.zip"},
    )


@app.post("/admin/close")
async def admin_close() -> JSONResponse:
    async with state_lock:
        state.current_question = -1
        event = format_state_event(-1)
    await broadcast(event)
    return JSONResponse({"ok": True, "question": -1})
