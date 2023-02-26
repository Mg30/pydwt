# pydwt

The pydwt library provides a set of tools for orchestrating tasks of data processing in a directed acyclic graph (DAG). This DAG is composed of tasks that have dependencies between them and can be executed in parallel or sequentially, depending on their dependencies.

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
from pydwt.sql.session import Session
from pydwt.sql.dataframe import DataFrame

session = Session(engine, schema="my_schema")

df = session.table("my_table")

# select some columns
df = df.select(df.col1, df.col2)

# filter rows based on a condition
df = df.where(df.col1 > 10)

# group by a column and aggregate another column
df = df.group_by(df.col2, agg={df.col1: (func.sum, "sum_col1")})

# show the resulting DataFrame
df.materialize("new_table", as_="table")

```
## Task

The `task.py` module defines a Task class for representing a task in the DAG. A Task object has a run method that is responsible for executing the task. You can also define the task's dependencies, schedule, and other parameters when creating the object.

To create a Task object, you can use the `@Task` decorator and define the run method. Here is an example of how to create a Task object:

```python
from pydwt.core.task import Task

@Task()
def task_one():
    df = session.table("features")
    df = df.with_column("new_column", case((df.preds == "hw", "W")))
    df.materialize("new_table", as_="table")


@Task(depends_on=[task_one])
def task_two():
    df = session.table("new_table")
    df = df.where((df.new_column == "W"))
    df = df.with_column("new_column", case((df.preds == "hw", "W")))
    df.show()

```

## Create a new pydwt project:

`pydwt new <my_project>`

This command will create a new project with the name "my_project" and the required file structure.
```
my_project/
    models/
        example.py
    dags/
settings.yml
```

* `project_name/models`: where you will put your tasks
* `project_name/dags/`: where the corresponding dag PNG file will be
* `project_name/settings.yml`: a configuration file for your project. This file includes the configuration options for your project, such as the path to your data directory.



## Export the DAG

`pydwt export-dag` 

will export the current state of your dag in the `project_name/dags/` as PNG file with timestamp.

## Run your project

`pydwt run`

will run the current state of your DAG. It will process the tasks in the DAG by level and parrelise
it with the `ThreadExecutor`

## Configuration of your pydwt project

The `settings.yml` file is a configuration file for your pydwt project. It stores various settings such as the project name, database connection details, and DAG tasks.

### connection
The connection section contains the configuration details for connecting to the database. The available options are:

* `db`: the name of the database
* `host`: the hostname or IP address of the database server
* `user`: the username for the database connection
* `password`: the password for the database connection
* `port`: the port number to use for the database connection
* `sql_alchemy_driver`: the SQLAlchemy driver to use for the database connection

### project
The project section contains the project-related settings. The available options are:

`name`: the name of the project

### tasks
This section contains the configuration for each task defined in the pydwt project.  

Each task is identified by its name, and the configuration is stored as a dictionary.  

The dictionary can contain any key-value pairs that the task implementation may need to use, but it must have a key named `materialize`.

* The `materialize` key specifies how the task output should be stored. The value can be either `view` or `table`.
The value of the materialize key determines whether the task output should be stored as a SQL view or a SQL table. If the value is view, the output is stored as a SQL view. If the value is table, the output is stored as a SQL table.

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
    print("somme processing")
```
### sources

The sources section contains the database sources that can be used in the project. Each source must have a unique name and specify the schema and table to use for the source.
## License
This project is licensed under GPL.