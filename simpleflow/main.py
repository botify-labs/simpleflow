#!/usr/bin/env python3
from __future__ import annotations

from enum import Enum

import typer
from typing_extensions import Annotated

from simpleflow.cli import workflow

app = typer.Typer(
    # add_completion=False,
    no_args_is_help=True,
    context_settings={"help_option_names": ["--help", "-h"]},
)

app.add_typer(workflow.app, name="workflow", help="Manage workflows")


class Format(str, Enum):
    json = "json"
    prettyjson = "prettyjson"
    csv = "csv"
    tsv = "tsv"
    tabular = "tabular"
    human = "human"


@app.callback()
def main(
    ctx: typer.Context,
    format: Annotated[Format, typer.Option("--format", "-f", envvar="SIMPLEFLOW_FORMAT")] = Format.json,
):
    ctx.params["format"] = format.lower()


# @app.command()
# def main(name: str):
#     print(f"Hello {name}")
#
#
# def run(function: Callable[..., Any]) -> None:
#     app = Typer(
#         # add_completion=False
#     )
#     app.command(context_settings={"help_option_names": ["-h", "--help"]})(function)
#     app()


if __name__ == "__main__":
    app()
    # run(main)
