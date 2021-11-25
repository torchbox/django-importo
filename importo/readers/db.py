from typing import Mapping, Sequence, Tuple

from django.db import connections

from .base import BasePaginatedReader


class RawSQLDatabaseReader(BasePaginatedReader):
    """
    A Reader that reads data from one of the databases configured
    in the project's `DATABASES` setting. Recieves the raw SQL to be
    run as `query` when initialising, and adds LIMIT and OFFSET to
    apply pagination.
    """

    requires_db_connection = True
    db_connection_help = (
        "The database to read data from (a key value from settings.DATABASES)."
    )
    default_page_size = 1000

    def __init__(
        self,
        query: str,
        source_db: str,
        page_size: int = None,
        start_page: int = None,
        stop_page: int = None,
        start_row: int = None,
        stop_row: int = None,
    ):
        self.query = query
        self.connection = connections[source_db]
        super().__init__(page_size, start_page, stop_page, start_row, stop_row)

    def fetch(
        self, page_number: int, start_row: int, stop_row: int = None
    ) -> Sequence[Mapping]:
        query = self.prepare_query(self.query, page_number, start_row, stop_row)
        response = self.execute_query(query)
        return response

    def prepare_query(
        self, query: str, page_number: int, start_row: int, stop_row: int
    ) -> str:
        if stop_row is None:
            limit = self.page_size
        else:
            limit = stop_row

        offset = start_row - 1
        if page_number > 1:
            offset += (page_number - 1) * self.page_size

        query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        return query

    def execute_query(self, prepared_query: str) -> Tuple[dict]:
        with self.connection.cursor() as cursor:
            cursor.execute(prepared_query)
            columns = [col[0] for col in cursor.description]
            results = tuple(dict(zip(columns, row)) for row in cursor.fetchall())
        return results
