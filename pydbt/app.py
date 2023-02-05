import typer
from pydbt.core.project import Project

app = typer.Typer()
project_handler = Project()


@app.command()
def new(project_name: str):
    project_handler.new(project_name)


@app.command()
def run():
    project_handler.run()


@app.command()
def export_dag():
    project_handler.export_dag()


if __name__ == "__main__":
    app()
