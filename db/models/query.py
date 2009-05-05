from django.db.models.query import QuerySet
from django.db import connection as default_connection

class MultiDBQuerySet(QuerySet):
    """
    QuerySet subclass that writes to the primary (Django default) database
    but reads from slave databases.
    
    We only have to override the `update` and `filter` methods. The `save`
    and `delete` methods import django.db.connection directly, so they'll
    operate on the default database regardless.
    """

    def filter(self, *args, **kwargs):
        """
        If we're filtering on the pk, make sure the query goes to the master.
        This is necessary because when you save() an object Django first
        calls filter(pk=<pk>) and in order to decide whether to insert or
        update. If we send these queries to the slave, a small amount of lag
        could result in double inserts and pk conflicts.
        """
        if 'pk' in kwargs:
            self.query.connection = default_connection
        return super(MultiDBQuerySet, self).filter(*args, **kwargs)

    def update(self, **kwargs):
        """
        Updates all elements in the current QuerySet, settinga ll given
        fields to the appropriate values. Gotta use the default (read/write)
        databases connection for this.
        """
        slave_conn = self.query.connection
        self.query.connection = default_connection
        super(MultiDBQuerySet, self).update(**kwargs)
        self.query.connection = slave_conn
    update.alters_data = True

    def _update(self, values):
        slave_conn = self.query.connection
        self.query.connection = default_connection
        super(MultiDBQuerySet, self)._update(values)
        self.query.connection = slave_conn
    _update.alters_data = True
