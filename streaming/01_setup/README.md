# 01 — Setup

Do this once after the Codespace starts. Three short steps; ~5 minutes.

> If anything goes wrong: see *Troubleshooting* at the bottom.

## What's already done for you

When the Codespace finishes building, the following is **already** running
or installed — you don't need to do any of it manually:

| Done automatically | How |
|---|---|
| Redpanda + Console + Flink + Dinky containers are up | `docker compose` via the devcontainer |
| `rpk` (Redpanda CLI) is on your `$PATH` | baked into the workspace image |
| Python 3.12 + `uv` are installed | baked into the workspace image |
| The Python venv with `confluent-kafka`, `faker` and `ipykernel` is created | `cd streaming && uv sync` runs in `postCreateCommand` |
| New terminals auto-activate the venv (you'll see `(deng-streaming)` in the prompt) | one-line append to `/root/.bashrc` in `postCreateCommand` |
| VS Code uses `streaming/.venv/bin/python` as the default Python | `python.defaultInterpreterPath` in `devcontainer.json` |

So the kernel for Jupyter is **already there** — `ipykernel` is in
[`pyproject.toml`](../pyproject.toml). The first time you open a notebook
VS Code should pick the right kernel automatically; if not, see
*Variant 1* below.

## Step 1 — Verify the cluster is up

Open a terminal in VS Code (*Terminal → New Terminal*) and run:

```bash
rpk cluster info -X brokers=redpanda:29092
```

You should see one broker line. If you get *connection refused*, the
stack is still starting — wait 30 s and retry.

## Step 2 — Pick the Python kernel for notebooks

Open [smoke_test.ipynb](smoke_test.ipynb).

1. Top-right corner of the notebook → **Select Kernel**.
2. **Python Environments…**
3. Pick the one that ends in `streaming/.venv/bin/python` (recommended).

You only do this once per notebook — VS Code remembers your choice.

### If `.venv` doesn't appear in the list

The venv lives in `streaming/.venv`, not at the workspace root, so VS
Code's auto-discovery sometimes misses it. Three ways around it
(**Variant 1** is the most reliable):

**Variant 1 — type the path directly**

1. **Select Kernel** → **Select Another Kernel…** → **Python
   Environments…** → **Enter interpreter path…**
2. Paste:
   ```
   /workspaces/DENG_FS26/streaming/.venv/bin/python
   ```
3. Confirm. The kernel picker now lists this entry — pick it.

**Variant 2 — refresh the discovery cache**

`F1` (or `Cmd/Ctrl+Shift+P`) → `Python: Clear Cache and Reload Window`
→ Enter. VS Code re-scans and now finds the venv.

**Variant 3 — verify the venv exists at all**

If even *Variant 1* fails with "interpreter not found", the venv was
never created. From a terminal:

```bash
ls -la streaming/.venv/bin/python   # should show a symlink to /usr/local/bin/python3
# if missing:
cd streaming && uv sync
```

## Step 3 — Run the smoke test

Run all cells in [smoke_test.ipynb](smoke_test.ipynb). It:

1. imports `confluent_kafka` (proves the venv is wired up),
2. connects to Redpanda and lists topics,
3. produces a "hello" event and reads it back.

If everything prints "OK", you're ready for the exercises.

## Step 4 — One-time Dinky setup

Needed only when you start [the Flink track](../03_exercise/flink_exercises.md).
See the dedicated walkthrough: [dinky_guide.md](dinky_guide.md).

## Troubleshooting

| Symptom | Fix |
|---|---|
| Notebook says *"Select Kernel"* but no `.venv` is offered | enter the path manually: *Select Another Kernel…* → *Enter interpreter path…* → `/workspaces/DENG_FS26/streaming/.venv/bin/python`. See *Step 2 → If `.venv` doesn't appear*. |
| `ModuleNotFoundError: confluent_kafka` | wrong kernel selected. *Select Kernel* → pick the `.venv` one. |
| `KafkaError{code=_TRANSPORT}` / "Broker not available" | the stack is still starting. Wait, then re-run. `docker ps` shows you which containers are up. |
| `docker: command not found` | the Codespace was built before docker was added. *F1 → Codespaces: Rebuild Container*. |
