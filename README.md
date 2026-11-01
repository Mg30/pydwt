# pydwt

Python data workflows combining SQLAlchemy Core transformations with directed
acyclic graph (DAG) orchestration. `Session` and `DataFrame` build SQL queries;
decorated tasks materialize results or perform other processing, and the workflow
executes tasks in dependency order using a thread executor.

## Status and compatibility

The `develop` branch is version **0.2.0a0**, an alpha source version. The latest
PyPI release is **0.1.1** (March 2023); this README describes the source branch.
Package metadata declares Python **>=3.8.1,<4.0**. CI configuration lists
Python 3.8–3.11, but uses the retired `ubuntu-18.04` runner. That configuration
does not establish current passing tests or support for newer Python versions.

## Installation

For the source version described here:

```bash
pip install 'pydwt @ git+https://github.com/Mg30/pydwt.git@develop'
```

For the published release, use `pip install pydwt==0.1.1`. Install the database
driver required by your SQLAlchemy connection URL separately.

## Guide

In this document, we will provide a brief explanation of the main modules of the pydwt library, which are:

* `session.py`: module for interacting with a database and creating DataFrame objects to manipulate data.
* `dataframe.py`: module for defining a DataFrame class for working with data.
* `task.py`: module for defining tasks in the DAG.
* `dag.py`: module for creating and traversing the DAG.
* `workflow.py`: module for running the DAG.


## Session

The `session.py` module is responsible for interacting with a database and creating DataFrame objects to manipulate data. To use this module, you need to create an instance of the Session class, passing an SQLAlchemy engine object and the schema of the database (if it has one). Then, you can use the table method to create a DataFrame object from a table in the database.

Here is an example of how to create a Session object and use the table method to create a DataFrame object:

```python
from sqlalchemy import create_engine
from pydwt.sql.session import Session

engine = create_engine("postgresql://user:password@localhost/dbname")
session = Session(engine, schema="my_schema")

df = session.table("my_table")
```

## DataFrame

The `dataframe.py` module defines a DataFrame class for working with data. A DataFrame object is essentially a table with labeled columns and rows. You can use it to perform operations such as selecting, filtering, grouping, and aggregating data.

You can also materialize a DataFrame as a table or view in the database by calling the materialize method.

Here is an example of how to create a DataFrame object and perform some operations on it:

```python
from sqlalchemy import func
from pydwt.sql.session import Session

session = Session(engine, schema="my_schema")

df = session.table("my_table")

# select some columns
df = df.select(df.col1, df.col2)

# filter rows based on a condition
df = df.where(df.col1 > 10)

# group by a column and aggregate another column
df = df.group_by(df.col2, agg={df.col1: (func.sum, "sum_col1")})

# persist the resulting query as a table
df.materialize("my_schema.new_table", as_="table")

```

This continues the `engine` setup above and assumes `my_table` contains `col1`
and `col2`. The DataFrame represents a SQL query, not an in-memory pandas table.
Materialization emits database-specific DDL: `table` drops and recreates the named
table; `view` uses `CREATE OR REPLACE VIEW`. Use a scratch schema when experimenting
and verify the emitted SQL for your database dialect.

## Task

The `task.py` module defines a Task class for representing a task in the DAG. A Task object has a run method that is responsible for executing the task. You can also define the task's dependencies, schedule, and other parameters when creating the object.

Use `@Task()` to register a Python function as a task. Declare upstream functions
with `depends_on`:

```python
from sqlalchemy import case
from pydwt.core.task import Task

@Task()
def task_one():
    df = session.table("features")
    df = df.with_column("new_column", case((df.preds == "hw", "W")))
    df.materialize("my_schema.new_table", as_="table")


@Task(depends_on=[task_one])
def task_two():
    df = session.table("new_table")
    df = df.where((df.new_column == "W"))
    df.show()

```

Place these task definitions in a generated project's `models` directory and run
them through the CLI, which initializes configuration and dependency injection.
The example uses the `session` defined above and a `features` table with a `preds`
column.

## Create a new pydwt project

`pydwt new <my_project>`

This command will create a new project with the name "my_project" and the required file structure.

```text
my_project/
    models/
        example.py
    dags/
settings.yml
```

* `project_name/models`: where you will put your tasks
* `project_name/dags/`: where the corresponding dag PNG file will be
* `settings.yml`: the project name, database connection, sources, and task configuration.



## Export the DAG

`pydwt export-dag`

will export the current state of your dag in the `project_name/dags/` as PNG file with timestamp.

## Run your project

```bash
pydwt run
pydwt run example.task_two
pydwt run example.task_two --with-dep
```

With no task name, the workflow runs the full DAG in dependency levels using
`ThreadExecutor`. A named task runs alone by default. Add `--with-dep` to include
its upstream dependencies. Failed parent tasks prevent dependent tasks from running.


## Test your connection setup

`pydwt test-connection`

will test the current database connection according to your `settings.yml` file.

## Configuration of your pydwt project

The `settings.yml` file is a configuration file for your pydwt project. It stores various settings such as the project name, database connection details, and DAG tasks.

### connection
The connection section contains the configuration details for connecting to the database. The available options are:

* `url`: the connection string to your db

You can add other keys that will be forwarded to the underlying `create_engine` function
for instance you can add a `echo : true` and it will call `create_engine(url=url, echo=echo)`
see [here](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine) supported args.

### project
The project section contains the project-related settings. The available options are:

`name`: the name of the project

### tasks
This section contains the configuration for each task defined in the pydwt project.  

Each task is identified by its name, and the configuration is stored as a dictionary.  

The dictionary contains key-value pairs consumed by your task implementation.
The generated example includes `materialize`; the workflow does not require or
automatically apply that setting.

For example, a task can read `materialize` and pass `view` or `table` to
`DataFrame.materialize(..., as_=...)`.

Each task implementation can access its configuration by injecting the config argument and specifying Provide[Container.config.tasks.<task_name>]. The injected config argument is a dictionary containing the configuration for the specified task. 

example :

```python
from pydwt.core.task import Task
from dependency_injector.wiring import inject, Provide
from pydwt.core.containers import Container

@Task()
@inject
def task_one(config:dict = Provide[Container.config.tasks.task_one]):
    print(config)

@Task(depends_on=[task_one])
def task_two():
    print("some processing")
```
### sources

The sources section contains the database sources that can be used in the project. Each source must have a unique name and specify the schema and table to use for the source.

```yaml
sources:
  name_alias:
    table: xxx
    schema: yyy
```

You can then request this data source in your tasks:

```python
from pydwt.core.task import Task
from dependency_injector.wiring import inject, Provide
from pydwt.core.containers import Container

@Task()
@inject
def task_one(
    config:dict = Provide[Container.config.tasks.task_one],
    repo= Provide[Container.datasources],
    ):
    df = repo.get_sources("name_alias")
```



## License
This project is licensed under [GPL-3.0](LICENSE).
