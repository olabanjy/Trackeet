# Generated by Django 3.0.5 on 2020-11-03 12:55

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0011_processedaccountfile_processedalbumfile_processedtrackfile'),
    ]

    operations = [
        migrations.AddField(
            model_name='processedaccountfile',
            name='file_doc_txt',
            field=models.FileField(blank=True, upload_to='documents/txt/processed_tracks'),
        ),
        migrations.AddField(
            model_name='processedalbumfile',
            name='file_doc_txt',
            field=models.FileField(blank=True, upload_to='documents/txt/processed_tracks'),
        ),
        migrations.AddField(
            model_name='processedtrackfile',
            name='file_doc_txt',
            field=models.FileField(blank=True, upload_to='documents/txt/processed_tracks'),
        ),
    ]
