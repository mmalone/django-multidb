from django import db
from multidb.db import connection
from multidb.db.models.query import MultiDBQuerySet

class SlaveDatabaseManager(db.models.Manager):
    def get_query_set(self):
        """
        Return a MultiDBQuerySet objet with its query
        tied to a random slave database.
        """
        return MultiDBQuerySet(self.model, query=self.create_query())

    def create_query(self):
        """
        Returns a new Query object connected to a
        slave database.
        """
        return db.models.sql.Query(self.model, connection)
