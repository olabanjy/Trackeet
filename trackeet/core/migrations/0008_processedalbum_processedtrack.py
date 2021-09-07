# Generated by Django 3.0.5 on 2020-05-01 22:25

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0007_accounting_vendor'),
    ]

    operations = [
        migrations.CreateModel(
            name='ProcessedAlbum',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('file_name', models.CharField(blank=True, max_length=600, null=True)),
                ('file_doc', models.FileField(blank=True, upload_to='album/')),
                ('process_time', models.DateTimeField(default=django.utils.timezone.now)),
                ('processed', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='ProcessedTrack',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('file_name', models.CharField(blank=True, max_length=600, null=True)),
                ('file_doc', models.FileField(blank=True, upload_to='track/')),
                ('process_time', models.DateTimeField(default=django.utils.timezone.now)),
                ('processed', models.BooleanField(default=False)),
            ],
        ),
    ]