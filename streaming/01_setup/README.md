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

So the kernel for Jupyter is **already there** — `ipykernel` is in
[`pyproject.toml`](../pyproject.toml). VS Code only needs to be told to
*use* it.

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
| Notebook says *"Select Kernel"* but no `.venv` is offered | from a terminal: `cd streaming && uv sync`. Then click *Select Kernel* again — VS Code re-scans. |
| `ModuleNotFoundError: confluent_kafka` | wrong kernel selected. *Select Kernel* → pick the `.venv` one. |
| `KafkaError{code=_TRANSPORT}` / "Broker not available" | the stack is still starting. Wait, then re-run. `docker ps` shows you which containers are up. |
| `docker: command not found` | the Codespace was built before docker was added. *F1 → Codespaces: Rebuild Container*. |
