# Generated by Django 3.0.5 on 2021-05-25 08:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('tracker', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='trackentries',
            name='export_file',
            field=models.FileField(blank=True, null=True, upload_to='documents/tracker/exports'),
        ),
    ]
