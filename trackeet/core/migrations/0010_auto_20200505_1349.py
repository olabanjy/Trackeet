# Generated by Django 3.0.5 on 2020-05-05 12:49

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0009_auto_20200503_1643'),
    ]

    operations = [
        migrations.AlterField(
            model_name='album',
            name='imprint_label',
            field=models.CharField(blank=True, max_length=600, null=True),
        ),
        migrations.AlterField(
            model_name='master_drmsys',
            name='label_name',
            field=models.CharField(blank=True, max_length=600, null=True),
        ),
    ]
