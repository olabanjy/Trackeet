# Generated by Django 3.0.5 on 2020-12-16 07:37

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0014_auto_20201103_1425'),
    ]

    operations = [
        migrations.AddField(
            model_name='processedaccountfile',
            name='file_name_txt',
            field=models.CharField(blank=True, max_length=600, null=True),
        ),
        migrations.AddField(
            model_name='processedalbumfile',
            name='file_name_txt',
            field=models.CharField(blank=True, max_length=600, null=True),
        ),
        migrations.AddField(
            model_name='processedtrackfile',
            name='file_name_txt',
            field=models.CharField(blank=True, max_length=600, null=True),
        ),
    ]