from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import exc
from sqlalchemy.orm.exc import *
from flask import current_app


from config import SHELLBASE_CONNECTION_STRING

Base = declarative_base()

class shellbase_db:
    def __init__(self, use_logging=True):
        self.dbEngine = None
        self.metadata = None
        self.Session = None
        self.connection = None

    def connectDB(self, connect_string, printSQL=False):
        try:
            # Connect to the database
            self.dbEngine = create_engine(connect_string, echo=printSQL)

            # metadata object is used to keep information such as datatypes for our table's columns.
            self.metadata = MetaData()
            self.metadata.bind = self.dbEngine

            self.Session = scoped_session(sessionmaker(bind=self.dbEngine))

            self.connection = self.dbEngine.connect()

            return (True)
        except (exc.OperationalError, exc.InterfaceError, Exception) as e:
            current_app.logger.exception(e)
        return (False)

    def remove_session(self):
        self.Session.remove()

    def disconnect(self):
        try:
            if self.Session is not None:
                self.Session.close()
            if self.connection is not None:
                self.connection.close()
            if self.dbEngine is not None:
                self.dbEngine.dispose()
        except Exception as e:
            current_app.logger.exception(e)

