# Generated by Django 3.0.5 on 2020-04-30 11:15

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0006_accounting_processedaccount'),
    ]

    operations = [
        migrations.AddField(
            model_name='accounting',
            name='vendor',
            field=models.CharField(blank=True, max_length=600, null=True),
        ),
    ]
