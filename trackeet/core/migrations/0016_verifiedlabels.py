# Generated by Django 3.0.5 on 2021-02-13 09:39

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0015_auto_20201216_0837'),
    ]

    operations = [
        migrations.CreateModel(
            name='VerifiedLabels',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('label_name', models.CharField(blank=True, max_length=600, null=True)),
                ('label_percentage', models.IntegerField(blank=True, null=True)),
                ('vendor', models.CharField(blank=True, max_length=200, null=True)),
            ],
        ),
    ]
