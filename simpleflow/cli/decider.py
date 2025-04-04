from __future__ import annotations

import typer
from typing_extensions import Annotated

from simpleflow.swf.process.decider import command

app = typer.Typer(no_args_is_help=True)


@app.command()
def start(
    ctx: typer.Context,
    workflows: Annotated[list[str] | None, typer.Argument()] = None,
    *,
    domain: Annotated[str, typer.Option(envvar="SWF_DOMAIN")],
    task_list: Annotated[str, typer.Option("--task-list", "-t")] | None = None,
    nb_processes: Annotated[int, typer.Option("--nb-processes", "-n")] | None = None,
):
    """
    Start a decider.
    """
    if not workflows and not task_list:
        raise typer.BadParameter("workflows or task_list is required")
    command.start(
        workflows=workflows or [],
        domain=domain,
        task_list=task_list,
        nb_processes=nb_processes,
    )


if __name__ == "__main__":
    app()
