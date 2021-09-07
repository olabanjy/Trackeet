# Generated by Django 3.0.5 on 2021-04-12 08:10

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0019_processedartistfile_processedlabelfile'),
    ]

    operations = [
        migrations.AddField(
            model_name='missingrecordsdoc',
            name='document',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='core.ProcessedDocument'),
        ),
        migrations.AddField(
            model_name='wronglabeldocs',
            name='document',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='core.ProcessedDocument'),
        ),
    ]