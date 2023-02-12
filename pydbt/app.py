import typer
import yaml
from pydbt.core.containers import Container

app = typer.Typer()
container = Container()
container.config.from_yaml("settings.yml", loader=yaml.UnsafeLoader)
container.wire(packages=["pydbt.core"])

project_handler = container.project_factory()


@app.command()
def new(project_name: str):
    project_handler.new(project_name)


@app.command()
def run():
    project_handler.run()


@app.command()
def export_dag():
    container.init_resources()
    project_handler.export_dag()


if __name__ == "__main__":
    app()



