from __future__ import annotations
from typing import List, Literal
from sqlalchemy import select
from pydbt.core.materializations import CreateTableAs


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

    def select(self, *args):
        """Create a new DataFrame that only has the columns specified.

        Args:
            *args (selectable): Columns to select from the dataframe.

        Returns:
            DataFrame: New DataFrame with only the selected columns.
        """
        projection = select(*args).cte()
        return DataFrame(projection, self._engine)

    def where(self, expr):
        """Create a new DataFrame that only has rows that meet the condition.

        Args:
            expr (sql.expression): SQLAlchemy expression to filter the rows.

        Returns:
            DataFrame: New DataFrame with only the rows that meet the condition.
        """
        selection = select(self._stmt).where(expr).cte()
        return DataFrame(selection, self._engine)

    def with_column(self, name, expr):
        """Create a new DataFrame that has an additional column.

        Args:
            name (str): Name of the new column.
            expr (sql.expression): SQLAlchemy expression to compute the values of the new column.

        Returns:
            DataFrame: New DataFrame with an additional column.
        """
        return DataFrame(select(self._stmt, expr.label(name)).cte(), self._engine)

    def group_by(self, *args, **kwargs) -> DataFrame:
        """Create a new DataFrame that has grouped rows.

        Args:
            *args (selectable): Columns to group by.
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

    def join(
        self, other: DataFrame, expr, how=Literal["inner", "left", "right", "full"]
    ):
        if how == "left":
            stmt = select(join(self._stmt, other._stmt, expr, isouter=True))
        elif how == "right":
            stmt = select(join(other._stmt, self._stmt, expr, isouter=True))
        elif how == "full":
            stmt = select(join(self._stmt, other._stmt, expr, full=True))
        else:
            stmt = select(join(self._stmt, other._stmt, expr))
        return DataFrame(stmt.cte(), self._engine)

    def show(self):
        conn = self._engine.connect()
        q = select(self._stmt).limit(20)
        print(conn.execute(q).fetchall())
        conn.close()

    def materialize_as_table(self, name):
        self._stmt = select(self._stmt)
        ctas = CreateTableAs(name, self._stmt)
        q = str(ctas.compile(bind=self._engine))
        conn = self._engine.connect()
        conn.execute(q)
        conn.close()
