from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable
from sqlalchemy.sql.selectable import Selectable
from typing import Any


class CreateTableAs(Executable, ClauseElement):
    """_summary_
    :param Executable: _description_
    :type Executable: _type_
    :param ClauseElement: _description_
    :type ClauseElement: _type_
    """

    inherit_cache = True

    def __init__(self, name: str, select_query: Selectable):
        self.name = name
        self.select_query = select_query


@compiles(CreateTableAs)
def visit_create_table_as(element: Any, compiler: Any, **kw: str) -> str:
    """_summary_
    :param element: _description_
    :type element: Any
    :param compiler: _description_
    :type compiler: Any
    :return: _description_
    :rtype: str
    """
    return """
DROP TABLE IF EXISTS {0};
CREATE TABLE {0}
AS
{1}
""".format(
        element.name,
        compiler.process(element.select_query, literal_binds=True),
    )


class CreateViewAs(Executable, ClauseElement):
    """_summary_
    :param Executable: _description_
    :type Executable: _type_
    :param ClauseElement: _description_
    :type ClauseElement: _type_
    """

    inherit_cache = True

    def __init__(self, name: str, select_query: Selectable):
        self.name = name
        self.select_query = select_query


@compiles(CreateViewAs)
def visit_create_view_as(element: Any, compiler: Any, **kw: str) -> str:
    """_summary_
    :param element: _description_
    :type element: Any
    :param compiler: _description_
    :type compiler: Any
    :return: _description_
    :rtype: str
    """
    return """
CREATE OR REPLACE VIEW {}
AS
{}
""".format(
        element.name,
        compiler.process(element.select_query, literal_binds=True),
    )
