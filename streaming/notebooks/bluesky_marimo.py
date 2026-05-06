import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb

    return duckdb, mo


@app.cell
def _(duckdb):
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(
        "ATTACH 'host=risingwave port=4566 user=root dbname=dev' AS rw (TYPE postgres)"
    )
    return (con,)


@app.cell
def _(con, mo):
    mo.md("## Posts per minute (RisingWave materialized view)")
    df = con.execute(
        """
        SELECT window_start, posts
        FROM rw.public.posts_per_minute
        ORDER BY window_start DESC
        LIMIT 30
        """
    ).df()
    df
    return (df,)


@app.cell
def _(con, mo):
    mo.md("## Top languages")
    langs = con.execute(
        """
        SELECT lang, posts
        FROM rw.public.top_languages
        ORDER BY posts DESC
        LIMIT 15
        """
    ).df()
    langs
    return (langs,)


@app.cell
def _(con, mo):
    mo.md("## Last 20 posts")
    last = con.execute(
        """
        SELECT event_ts, text, langs
        FROM rw.public.bluesky_posts
        ORDER BY event_ts DESC
        LIMIT 20
        """
    ).df()
    last
    return (last,)


if __name__ == "__main__":
    app.run()
