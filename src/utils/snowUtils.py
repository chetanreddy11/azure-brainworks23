import snowflake.connector

class SnowflakeConnector:
    def __init__(self, account, user, paswword, warehouse, database, schema):
        """AI is creating summary for __init__

        Arg:
            account([type]): [description]
            user([type]): [description]
            password([type]): [description]
            warehouse([type]): [description]
            database([type]): [description]
            schema([type]): [description]
        """
        self.account = account
        self.user = user
        self.password = paswword
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None
        self.cursor = None

    def connect(self):
        """"
        Establish  a cnnection to snowflake
        """
        self.connection = snowflake.connector.connect(
            user = self.user,
            account = self.account,
            password = self.password,
            warehouse = self.warehouse,
            database = self.database,
            schema = self.schema
        )
        self.cursor = self.connection.cursor()

    def execute_query(self,query):
        """"
        Excute a SQL query
        """
        if not self.connection or not self.cursor:
            raise Exception("Connection not established call connect() method first")
        
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def close_connection(self):
        """"
        Close the connection
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()