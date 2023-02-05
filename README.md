# PyDBT
PyDBT is a data processing tool written in python. It provides an interface on top of SQLAlchemy core version 1.4 and greater, task dependencies handling, task retry on error, and task caching.


## Usage

## Create the models Directory:
The first step in using the Task class and the DataFrame class is to create a models directory in your framework. This directory will contains file with you processing logic. In this file functions decorated with the `@Task` will be marked as task to be processed.

## Create a new  processing file
Start by creating a file  `some_processing.py` under `models/`


## Create a new session
First you need a engine and a session to interact with the db: 
```python
from sqlalchemy import create_engine
from sql.session import Session

engine = create_engine('postgres:///<con>')

# create a session with the engine and schema
session = Session(engine, schema='example_schema')
```

the session object will return a `DataFrame` Object. 
The DataFrame class is an interface that allows you to manipulate data using SQL-like operations on top of SQLAlchemy core.

### Write code with DataFrame wrapper

Here some code snippet that show how to use DataFrame

```python

df_a = session.table("tableA")
df_b = session.table("tableB")

df_joined = df_a.join(
        df_b,
        (df_a.key == df_b.key)
        & (df_a.date == df_b.date_right),
    )

df_joined.select(df_joined.key, df_joined.date_right).show() # trigger connection

```


## Decorate Your Functions with `@Task`

Then you can start writing functions that you wrap you processing logic. 

The function you write do **NOT** need to use the `DataFrame` class. This class is just
an helper so if you want to write plain SQL alchemy core code it will work too.

The `@Task` decorator will allow your framework to identify and load these functions as tasks. To do this, you simply need to decorate each function with the @Task decorator.

```python

from core.task import Task

@Task()
def processing():
    df_a = session.table("tableA")
    df_b = session.table("tableB")
    df_joined = df_a.join(
            df_b,
            (df_a.key == df_b.key)
            & (df_a.date == df_b.date_right),
        )
    df_joined.select(df_joined.key, df_joined.date_right)
    df_joined.write("new_table")

```

You can specify dependencies between tasks 


```python


@Task()
def processing():
    df_a = session.table("tableA")
    df_b = session.table("tableB")
    df_joined = df_a.join(
            df_b,
            (df_a.key == df_b.key)
            & (df_a.date == df_b.date_right),
        )
    df_joined.select(df_joined.key, df_joined.date_right)
    df_joined.write("new_table")

@Task(depends_on[processing])
def child_task():
    df = session.table("new_table")
    df.show()
```
Here is a more complete example that you can find in the `models`directory

```python
from sql.session import Session
from sqlalchemy import func, case
from core.task import Task
from core.schedule import Monthly
from connections.postgresql import get_engine

engine = get_engine()
session = Session(engine=engine,schema="public")


@Task(ttl_minutes=60)
def task_one():
    df = session.table("predictions")
    df.show()


@Task(depends_on=[task_one])
def task_two():
    df = session.table("predictions")
    df = df.where((df.matchDate == "2022-12-30"))
    df = df.with_column("new_column", case((df.preds == "hw", "W")))
    df = df.group_by(df.matchDate, df.preds, agg={"preds": (func.count, "count_preds")})
    df.show()


@Task(depends_on=[task_one])
def task_three():
    df = session.table("predictions")
    df = df.where((df.matchDate == "2022-12-31"))
    df = df.with_column("new_column", case((df.preds == "hw", "W")))
    df = df.group_by(df.matchDate, df.preds, agg={"preds": (func.count, "count_preds")})
    df.show()


@Task(depends_on=[task_three, task_two], runs_on=Monthly())
def task_four():
    df = session.table("predictions")
    df = df.where((df.matchDate == "2022-12-31"))
    df.show()
```

Once registred tasks will be loaded and a DAG will be generated. you can export and visualize the dag using:

`python app.py export-dag .`

**Example** :   
![alt](dag.png)

The DAG allow the engine to process tasks with the same level of dependencies
in paralell using the `core.executors.ThreadExecutor`

Using this dag logs this :

```
INFO 02/03/2023 05:13:50 PM: registring task task_one
INFO 02/03/2023 05:13:50 PM: registring task task_two
INFO 02/03/2023 05:13:50 PM: registring task task_three
INFO 02/03/2023 05:13:50 PM: registring task task_four
INFO 02/03/2023 05:13:50 PM: registring task task_five
INFO 02/03/2023 05:13:50 PM: cache enable fetch latest file
INFO 02/03/2023 05:13:50 PM: using latest run task_20230203_16:58:57.pkl
INFO 02/03/2023 05:13:50 PM: comparing tasks
INFO 02/03/2023 05:13:50 PM: no added task
INFO 02/03/2023 05:13:50 PM: task_one is equal to previous run keeping old task
INFO 02/03/2023 05:13:50 PM: task_two is equal to previous run keeping old task
INFO 02/03/2023 05:13:50 PM: task_three is equal to previous run keeping old task
INFO 02/03/2023 05:13:50 PM: task_four is equal to previous run keeping old task
INFO 02/03/2023 05:13:50 PM: task_five is equal to previous run keeping old task
INFO 02/03/2023 05:13:50 PM: exploring dag level 1
INFO 02/03/2023 05:13:50 PM: collected tasks set ['task_one'] using 1 threads
INFO 02/03/2023 05:13:50 PM: task task_one is scheduled to be run
INFO 02/03/2023 05:13:50 PM: task task_one: ttl not elasped: skipping
INFO 02/03/2023 05:13:50 PM: exploring dag level 2
INFO 02/03/2023 05:13:50 PM: collected tasks set ['task_two', 'task_three'] using 2 threads
INFO 02/03/2023 05:13:50 PM: task task_two is scheduled to be run
INFO 02/03/2023 05:13:50 PM: task task_three is scheduled to be run
ERROR 02/03/2023 05:13:50 PM: task  task_three failed after 0 attempts: 'matchDate'
[(datetime.date(2022, 12, 30), 'd', 2), (datetime.date(2022, 12, 30), 'aw', 1), (datetime.date(2022, 12, 30), 'hw', 3)]
INFO 02/03/2023 05:13:50 PM: exploring dag level 3
INFO 02/03/2023 05:13:50 PM: collected tasks set ['task_four'] using 1 threads
INFO 02/03/2023 05:13:50 PM: task task_four is not scheduled to be run: skipping
INFO 02/03/2023 05:13:50 PM: exploring dag level 4
INFO 02/03/2023 05:13:50 PM: collected tasks set ['task_five'] using 1 threads
INFO 02/03/2023 05:13:50 PM: task task_five is scheduled to be run
INFO 02/03/2023 05:13:50 PM: retrying task task_five try number: 0
INFO 02/03/2023 05:13:50 PM: retrying task task_five try number: 1
INFO 02/03/2023 05:13:50 PM: retrying task task_five try number: 2
INFO 02/03/2023 05:13:50 PM: retrying task task_five try number: 3
INFO 02/03/2023 05:13:50 PM: retrying task task_five try number: 4
ERROR 02/03/2023 05:13:50 PM: task  task_five failed after 5 attempts: Erreur
```
First the engine collect all the tasks it finds in the `models`directory and then 
build a dag from their dependencies.
If cache is enable it will compare old and new tasks definition.
Then it will execute tasks with threads according to the level in the DAG.


## API Reference

### Task
The Task class represents a task in the data processing pipeline and provides the following properties:

* depends_on: A list of other tasks that this task depends on.
* runs_on: A schedule for running this task. The default is Daily().
* retry: The number of times to retry this task in case of failure.
* ttl_minutes: Time-to-live in minutes. If a positive value is provided, the task will only run if the time elapsed since the last run is greater than or equal to this value.

### DatFrame

The DataFrame class is a representation of a database table in the form of a Spark dataframe. It is used to perform SQL operations on the table.

The class constructor takes two arguments: base and engine. base is a SQLAlchemy query object that represents the SELECT statement to retrieve data from the database table. engine is a SQLAlchemy engine object that represents the database connection.

The class provides methods for executing SQL operations, such as filtering and aggregating data, as well as for retrieving the resulting data in the form of a Spark dataframe.

Here is an example of how to create a DataFrame object from SQL alchemy statement 

```python

from sqlalchemy import select, Table, MetaData
from sql.dataframe import DataFrame

engine = create_engine("sqlite:///example.db")
metadata = MetaData(schema='example_schema')
table = Table('example_table', metadata, autoload_with=engine)
base = select(table).cte()
df = DataFrame(base, engine)

```
Or you can use the `table`methode from `Session`

```python

session = Session(engine=engine,schema="public")
df = session.table("predictions")
df.show()

```

## License
This project is licensed under GPL.