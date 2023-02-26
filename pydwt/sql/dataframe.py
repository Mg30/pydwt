from __future__ import annotations
from typing import List, Literal
import sqlalchemy
from sqlalchemy import select, join
from pydwt.sql.materializations import CreateTableAs, CreateViewAs


class DataFrame(dict):
    """DataFrame class is an interface that allows to manipulate data
    using SQL-like operations on top of SQLAlchemy core.

    Args:
        base (selectable): Initial SQLAlchemy selectable object.
        engine (Engine): SQLAlchemy engine to execute the SQL commands.

    Properties:
        columns (List[str]): Names of the columns in the dataframe.
    """

    __getattr__ = (
        dict.__getitem__
    )  # overwrite `__getattr__` to allow access to values using dot notation

    def __init__(self, base, engine):
        """Initialize the DataFrame object."""
        self._stmt = base
        self._engine = engine
        for col in self._stmt.columns:
            name = str(col).split(".")[-1]
            self[name] = getattr(self._stmt.c, name)

    @property
    def columns(self) -> List[str]:
        """Return a list of the column names in the dataframe."""
        return [str(col).split(".")[-1] for col in self._stmt.columns]

    def select(self, *args) -> DataFrame:
        """Create a new DataFrame that only has the columns specified.

        Args:
            *args (selectable): Columns to select from the dataframe.

        Returns:
            DataFrame: New DataFrame with only the selected columns.
        """
        projection = select(*args).cte()
        return DataFrame(projection, self._engine)

    def where(self, expr) -> DataFrame:
        """Create a new DataFrame that only has rows that meet the condition.

        Args:
            expr (sql.expression): SQLAlchemy expression to filter the rows.

        Returns:
            DataFrame: New DataFrame with only the rows that meet the condition.
        """
        selection = select(self._stmt).where(expr).cte()
        return DataFrame(selection, self._engine)

    def with_column(self, name, expr) -> DataFrame:
        """Create a new DataFrame that has an additional column.

        Args:
            name (str): Name of the new column.
            expr (sql.expression): SQLAlchemy expression to compute the values of the new column.

        Returns:
            DataFrame: New DataFrame with an additional column.
        """
        return DataFrame(select(self._stmt, expr.label(name)).cte(), self._engine)

    def with_column_renamed(self, old_name: str, new_name: str) -> DataFrame:
        """Create a new DataFrame with the given column renamed.

        Args:
            old_name (str): Name of the column to be renamed.
            new_name (str): New name of the column.

        Returns:
            DataFrame: New DataFrame with the given column renamed.
        """
        cols = [
            self[old_name].label(new_name) if col == old_name else self[col]
            for col in self.columns
        ]
        stmt = select(*cols)
        return DataFrame(stmt.cte(), self._engine)

    def drop(self, *args) -> DataFrame:
        """Returns a new DataFrame object with the specified columns removed.

        Args:
            *args: variable length argument list containing the column names to remove.

        Returns:
            A new DataFrame object with the specified columns removed.
        """
        cols = [self[col] for col in args if col not in args]
        stmt = select(*cols)
        return DataFrame(stmt.cte(), self._engine)

    def group_by(self, *args, **kwargs) -> DataFrame:
        """Create a new DataFrame that has grouped rows.

        Args:
            *args (Selectable): Columns to group by.
            **kwargs:
                agg (Dict[str, Tuple[Callable, str]]): Dictionary that maps from column name to
                (aggregation function, name of the aggregation).

        Returns:
            DataFrame: New DataFrame with grouped rows.
        """
        grouped = None
        agg = kwargs.get("agg", None)
        if agg:
            agg_expr = []
            for col_name, agg in agg.items():
                agg_func, agg_name = agg
                agg_expr.append(agg_func(col_name).label(agg_name))
            agg_expr = [*args, *agg_expr]
            grouped = select(*agg_expr).group_by(*args).cte()
        else:
            grouped = select(*args).group_by(*args).cte()
        return DataFrame(grouped, self._engine)

    def join(self, other: "DataFrame", expr, how: str = "inner") -> DataFrame:
        """
        Join this DataFrame with another DataFrame.

        Args:
            other (DataFrame): The other DataFrame to join with.
            expr: The join expression.
            how (str): The type of join. Can be "inner", "left", "right", or "full".

        Returns:
            DataFrame: A new DataFrame with the result of the join.
        """
        # Check that the join type is valid
        if how not in ["inner", "left", "right", "full"]:
            raise ValueError(f"Unsupported join type {how}.")

        # Perform the join operation
        if how == "left":
            stmt = select(join(self._stmt, other._stmt, expr, isouter=True))
        elif how == "right":
            stmt = select(join(other._stmt, self._stmt, expr, isouter=True))
        elif how == "full":
            stmt = select(join(self._stmt, other._stmt, expr, full=True))
        else:
            stmt = select(join(self._stmt, other._stmt, expr))

        # Return the result as a new DataFrame
        return DataFrame(stmt.cte(), self._engine)

    def show(self) -> None:
        """
        Print the first 20 rows of the DataFrame.
        """
        conn = self._engine.connect()
        q = select(self._stmt).limit(20)
        print(conn.execute(q).fetchall())
        conn.close()

    def to_cte(self) -> sqlalchemy.sql.selectable.CTE:
        "Return the DataFrame as a SQLAlchemy cte"
        return self._stmt

    def materialize(self, name: str, as_: Literal["view", "table"]) -> None:
        """
        Materialize the query as a table or view in the database.

        Args:
            name (str): The name of the table or view to create.
            as_ (Literal["view", "table"]): The type of object to create.

        Raises:
            ValueError: If an unsupported materialization type is specified.

        """
        # Convert the statement to a SELECT statement
        self._stmt = select(self._stmt)

        if as_ == "view":
            materialization = CreateViewAs(name, self._stmt)
        elif as_ == "table":
            materialization = CreateTableAs(name, self._stmt)
        else:
            # Raise an error if an unsupported materialization type is specified
            raise ValueError(f"Unsupported materialization type: {as_}")

        # Execute the materialization query
        q = materialization.compile(bind=self._engine)
        conn = self._engine.connect()
        conn.execute(q)
        conn.commit()
        conn.close()

    def collect(self) -> List[dict]:
        """
        Retrieve all the data in the dataframe as a list.

        Returns:
            List[dict]: A list of dictionaries, where each dictionary represents a row in the dataframe.
        """
        conn = self._engine.connect()
        result = conn.execute(select(self._stmt)).fetchall()
        conn.close()
        return result
