# Project Instructions

## Language
- All code, comments, and documentation must be in **English**

## Git Commits
- Do not include promotional lines in commit messages (no "Co-Authored-By", no tool advertisements)
- Keep commit messages concise and descriptive

## Databricks Notebooks
- Format: `.py` (Python) or `.sql` (SQL) using Databricks notebook source format
- Cell separators: `# COMMAND ----------`
- Markdown cells: `# MAGIC %md`
- Cross-language cells: `# MAGIC %sql`, `# MAGIC %python`
- Default catalog: `workspace`
- Exercises use `raise NotImplementedError(...)` for student tasks
- Solutions are stored in `solution/` folders and listed in `.gitignore`

## Deployment
- Databricks Asset Bundles with profile `free`
- Deploy: `databricks bundle deploy -p free`
- No `--target` flag needed (default target is `prod`)
