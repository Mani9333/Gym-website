import psycopg2
from psycopg2.extras import RealDictCursor
import logging

class PostgresDbOperationError(Exception):
    """Base exception class for PostgreSQL operations."""
    pass

class PostgresDbConnectionError(PostgresDbOperationError):
    """Raised for database connection errors."""
    pass

class PostgresDbQueryExecutionError(PostgresDbOperationError):
    """Raised for query execution errors."""
    pass
    """Custom exception class for database operations."""
    pass

class PostgresHelper:
    def __init__(self, host, port, database, user, password, logger=None, autocommit=True, use_dict_cursor=False):
        """
        Initializes the PostgreSQL helper object and establishes a connection to the database.

        :param host: Hostname of the PostgreSQL server
        :param port: Port of the PostgreSQL server
        :param database: Database name
        :param user: Username for authentication
        :param password: Password for authentication
        :param logger: Logger instance to use (optional)
        :param autocommit: Whether to enable autocommit mode (default is True)
        :param use_dict_cursor: If True, use RealDictCursor to return rows as dictionaries. If False (default), return rows as tuples.

        Using context manager:
        This class supports the context manager protocol, allowing users to manage resources more effectively.
        Example:
        with PostgresHelper(host="localhost", port=5432, database="mydb", user="user", password="pass") as db:
            rows = db.select_execute_query("SELECT * FROM my_table;")
            print(rows)
        # Automatically closes the connection and cursor when the block ends.

        Without context manager:
        If not using a `with` statement, users must manually call the `close()` method to clean up resources.
        Example:
        db = PostgresHelper(host="localhost", port=5432, database="mydb", user="user", password="pass")
        try:
            rows = db.select_execute_query("SELECT * FROM my_table;")
            print(rows)
        finally:
            db.close()

        Example usage of `use_dict_cursor`:
        - If `use_dict_cursor` is True:
          The results of SELECT queries will be returned as a list of dictionaries.
          Example:
            rows = db.select_execute_query("SELECT * FROM my_table;")
            print(rows[0]['column_name'])
        
        - If `use_dict_cursor` is False:
          The results of SELECT queries will be returned as a list of tuples.
          Example:
            rows = db.select_execute_query("SELECT * FROM my_table;")
            print(rows[0][0])
        """
        self.connection = None
        self.cursor = None
        self.connection.autocommit = autocommit
        self.logger = logger or self._initialize_logger()
        self.use_dict_cursor = use_dict_cursor
        try:
            self.connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            self.connection.autocommit = autocommit
            cursor_factory = RealDictCursor if use_dict_cursor else None
            self.cursor = self.connection.cursor(cursor_factory=cursor_factory)
            self.logger.info("Database connection established successfully.")
        except Exception as e:
            self.logger.error(f"Error connecting to the database: {e}")
            raise PostgresDbConnectionError(f"Error connecting to the database: {e}")

    def _initialize_logger(self):
        logger = logging.getLogger("PostgresHelper")
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def __enter__(self):
        """
        Enables usage of the class as a context manager.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Ensures that the connection and cursor are properly closed when exiting the context.
        """
        self.close()

    def select_execute_query(self, query, params=None):
        """
        Executes a query and returns the result as rows if applicable.

        :param query: SQL query to execute
        :param params: Parameters for the query (optional)
        :return: Query result rows (if any)

        Example usage:
        rows = db.select_execute_query("SELECT * FROM my_table;")
        if db.use_dict_cursor:
            print(rows)  # [{'column1': 'value1', 'column2': 'value2'}, ...]
            print(rows[0]['column1'])  # Access a specific column by name
        else:
            print(rows)  # [('value1', 'value2'), ...]
            print(rows[0][0])  # Access a specific column by index
        """
        try:
            self.cursor.execute(query, params)
            if query.strip().lower().startswith("select"):
                return self.cursor.fetchall()
            return None
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            self.connection.rollback()
            raise PostgresDbQueryExecutionError(f"Error executing query: {e}")

    def select_get_count_query(self, query, params=None):
        """
        Gets the count of rows based on a custom query.

        :param query: SQL query to count rows (e.g., "SELECT COUNT(*) FROM table_name WHERE condition")
        :param params: Parameters for the query (optional)
        :return: Row count

        Example usage:
        count_query = "SELECT COUNT(*) as count FROM my_table WHERE column_name = %s;"
        count = db.select_get_count_query(count_query, ("value",))
        print(f"Row count: {count}")
        """
        try:
            self.cursor.execute(query, params)
            result = self.cursor.fetchone()
            return result['count'] if self.use_dict_cursor else result[0]
        except Exception as e:
            self.logger.error(f"Error getting row count: {e}")
            self.connection.rollback()
            raise PostgresDbQueryExecutionError(f"Error getting row count: {e}")

    def insert(self, query, params=None):
        """
        Executes an insert query.

        :param query: SQL insert query
        :param params: Parameters for the query (optional)

        Example usage:
        db.insert("INSERT INTO my_table (column1, column2) VALUES (%s, %s);", ("value1", "value2"))
        """
        try:
            self.cursor.execute(query, params)
            if self.connection.autocommit:
                self.connection.commit()
        except Exception as e:
            self.logger.error(f"Error executing insert: {e}")
            self.connection.rollback()
            raise PostgresDbQueryExecutionError(f"Error executing insert: {e}")

    def bulk_insert(self, query, data, chunk_size=50):
        """
        Executes a bulk insert query in chunks using executemany. This allows handling large data sets efficiently.

        :param query: SQL insert query
        :param data: List of tuples containing the data to insert
        :param chunk_size: Number of rows to insert in each chunk (default is 50)

        Example usage:
        bulk_data = [("value1", "value2"), ("value3", "value4")]
        db.bulk_insert("INSERT INTO my_table (column1, column2) VALUES (%s, %s);", bulk_data)
        """
        try:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                self.cursor.executemany(query, chunk)
            if self.connection.autocommit:
                self.connection.commit()
        except Exception as e:
            self.logger.error(f"Error executing bulk insert: {e}")
            self.connection.rollback()
            raise PostgresDbQueryExecutionError(f"Error executing bulk insert: {e}")

    def update_execute_query(self, query, params=None):
        """
        Executes an update query.

        :param query: SQL update query
        :param params: Parameters for the query (optional)

        Example usage:
        db.update_execute_query("UPDATE my_table SET column1 = %s WHERE id = %s;", ("new_value", 1))
        """
        try:
            self.cursor.execute(query, params)
            if self.connection.autocommit:
                self.connection.commit()
        except Exception as e:
            self.logger.error(f"Error executing update: {e}")
            self.connection.rollback()
            raise PostgresDbQueryExecutionError(f"Error executing update: {e}")

    def delete_execute_query(self, query, params=None):
        """
        Executes a delete query.

        :param query: SQL delete query
        :param params: Parameters for the query (optional)

        Example usage:
        db.delete_execute_query("DELETE FROM my_table WHERE id = %s;", (1,))
        """
        try:
            self.cursor.execute(query, params)
            if self.connection.autocommit:
                self.connection.commit()
        except Exception as e:
            self.logger.error(f"Error executing delete: {e}")
            self.connection.rollback()
            raise PostgresDbQueryExecutionError(f"Error executing delete: {e}")

    def commit(self):
        """
        Commits the current transaction.
        """
        try:
            self.connection.commit()
        except Exception as e:
            self.logger.error(f"Error committing transaction: {e}")
            raise PostgresDbQueryExecutionError(f"Error committing transaction: {e}")

    def rollback(self):
        """
        Rolls back the current transaction.
        """
        try:
            self.connection.rollback()
        except Exception as e:
            self.logger.error(f"Error rolling back transaction: {e}")
            raise PostgresDbQueryExecutionError(f"Error rolling back transaction: {e}")

    def close(self):
        """
        Closes the database connection.
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
                self.logger.info("Database connection closed.")
        except Exception as e:
            self.logger.error(f"Error closing connection: {e}")
            raise PostgresDbConnectionError(f"Error closing connection: {e}")

# Example usage
if __name__ == "__main__":
    try:
        with PostgresHelper(
            host="localhost",
            port=5432,
            database="mydb",
            user="myuser",
            password="mypassword",
            autocommit=False,
            use_dict_cursor=True
        ) as db:

            # Example: Fetch rows
            rows = db.select_execute_query("SELECT * FROM my_table;")
            if db.use_dict_cursor:
                print(rows)  # [{'column1': 'value1', 'column2': 'value2'}, ...]
                print(rows[0]['column1'])  # Access a specific column by name
            else:
                print(rows)  # [('value1', 'value2'), ...]
                print(rows[0][0])  # Access a specific column by index

            # Example: Get row count
            count_query = "SELECT COUNT(*) as count FROM my_table WHERE column_name = %s;"
            count = db.select_get_count_query(count_query, ("value",))
            print(f"Row count: {count}")

            # Example: Update rows
            db.update_execute_query("UPDATE my_table SET column_name = %s WHERE id = %s", ("value", 1))

            # Example: Delete rows
            db.delete_execute_query("DELETE FROM my_table WHERE id = %s", (1,))

    except Exception as e:
        print(f"An error occurred: {e}")
