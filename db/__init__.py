from django.core import signals
from django.db.backends.mysql.base import DatabaseWrapper, CursorWrapper, Database, django_conversions
from django.utils.safestring import SafeString, SafeUnicode
from django.conf import settings

def pick_random_slave(slaves):
    """
    Randomly selects a slave database to connect to from a list
    of (<weight>, <db settings>) tuples.

    The chance of a particular database being selected is (statistically)
    equal to <weight>/sum(<all weights>).
    """
    import random
    r = random.random() * sum(i[0] for i in slaves)
    i = 0
    for weight, slave in slaves:
        i += weight
        if i >= r:
            return slave

class SlaveDatabaseWrapper(DatabaseWrapper):
    """
    Subclass of DatabaseWrapper, of which `django.db.connection` is
    an instance. Instances of this class are `connection` objects 
    that can be used to query slave databases.
    """

    def __init__(self, **kwargs):
        super(SlaveDatabaseWrapper, self).__init__(**kwargs)

    def _cursor(self, settings):
        """
        Override the default implementation of `_cursor` which 
        connects to the default database configured in settings.
        """
        if not self._valid_connection():
            kwargs = {
                'conv': django_conversions,
                'charset': 'utf8',
                'use_unicode': True,
            }
            kwargs = pick_random_slave(settings.SLAVE_DATABASES)
            self.connection = Database.connect(**kwargs)
            self.connection.encoders[SafeUnicode] = self.connection.encoders[unicode]
            self.connection.encoders[SafeString] = self.connection.encoders[str]
        cursor = CursorWrapper(self.connection.cursor())
        return cursor

connection = SlaveDatabaseWrapper()

# Register an event that closes the slave database connection
# when a Django request is finished
def reset_queries(**kwargs):
    connection.queries = []

# Register an devent that closes the database connection
# when a Django request is finished.
def connection_close(**kwargs):
    connection.close()

signals.request_started.connect(reset_queries)
signals.request_finished.connect(connection_close)
