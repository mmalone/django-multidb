from django.db import models
from django.contrib.auth.models import User
from multidb.db.models.manager import SlaveDatabaseManager

class Frob(models.Model):
    thing = models.CharField(max_length=32)
    owner = models.ForeignKey(User)

    objects = SlaveDatabaseManager()
