from django.db import models
from django.conf import settings
from django.utils import timezone

# Create your models here.


class TrackEntries(models.Model):
    title = models.CharField(null=True, blank=True, max_length=500)
    vid_id = models.CharField(null=True, blank=True, max_length=200)
    url = models.CharField(null=True, blank=True, max_length=500)
    duration = models.CharField(null=True, blank=True, max_length=500)
    thumbnail = models.CharField(null=True, blank=True, max_length=500)
    total_views = models.CharField(max_length=500, blank=True, null=True)
    genre = models.CharField(null=True, blank=True, max_length=500)
    likes = models.CharField(null=True, blank=True, max_length=500)
    dislikes = models.CharField(null=True, blank=True, max_length=500)
    commentCount = models.CharField(null=True, blank=True, max_length=500)
    artist = models.CharField(null=True, blank=True, max_length=500)
    export_file = models.FileField(upload_to='documents/tracker/exports', blank=True, null=True)

    def __str__(self):
        return self.title