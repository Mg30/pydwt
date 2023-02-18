from typing import List, Dict, Any
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    select,
    literal_column,
    MetaData,
    func,
    ForeignKey,
)
from pydbt.sql.dataframe import DataFrame
from pydbt.sql.session import Session
import pytest


# Define a fixture that creates a fake table using SQLite
@pytest.fixture(scope="session")
def session():
    engine = create_engine("sqlite:///:memory:", echo=True)
    metadata = MetaData()

    t = Table(
        "users",
        metadata,
        Column("user_id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer),
    )

    products = Table(
        "products",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("user_id", Integer, ForeignKey("users.user_id")),
    )

    metadata.create_all(bind=engine)

    # Insert some data into the table
    conn = engine.connect()
    conn.execute(
        t.insert(),
        [
            {"user_id": 1, "name": "Alice", "age": 25},
            {"user_id": 2, "name": "Bob", "age": 30},
            {"user_id": 3, "name": "Charlie", "age": 35},
            {"user_id": 4, "name": "David", "age": 40},
            {"user_id": 5, "name": "Eve", "age": 45},
        ],
    )
    conn.execute(
        products.insert(),
        [
            {"name": "Product A", "user_id": 1},
            {"name": "Product B", "user_id": 2},
            {"name": "Product C", "user_id": 2},
            {"name": "Product D", "user_id": 3},
            {"name": "Product E", "user_id": 4},
            {"name": "Product F", "user_id": 5},
        ],
    )
    conn.commit()
    conn.close()

    session = Session(engine)

    yield session

    metadata.drop_all(bind=engine)
    engine.dispose()


def test_dataframe_select(session):
    df = session.table("users")
    df2 = df.select(df.name, df.age)

    assert df2.columns == ["name", "age"]


def test_dataframe_where(session):
    df = session.table("users")
    df2 = df.where(literal_column("age") > 30)

    assert len(df2) == 3


def test_dataframe_with_column(session):
    df = session.table("users")
    df2 = df.with_column("age2", literal_column("age") * 2)

    assert df2.columns == ["user_id", "name", "age", "age2"]


def test_dataframe_group_by(session):
    df = session.table("users")
    df2 = df.group_by(df.age, agg={"user_id": (func.min, "min_id")})
    assert df2.columns == ["age", "min_id"]


def test_dataframe_with_column_renamed(session):
    df1 = session.table("users")
    df1 = df1.with_column_renamed("name","user_name")

    assert df1.columns == ['user_id', 'user_name', 'age']

def test_dataframe_join(session):
    df1 = session.table("users")
    df1 = df1.with_column_renamed("name","user_name")
    df2 = session.table("products")
    df2 = df2.with_column_renamed("name","product_name")
    df2 = df2.with_column_renamed("user_id","user_id_")
    df3 = df1.join(df2, (df1.user_id == df2.user_id_))
    
    df3.select(df3.age)

    assert df3.collect()
