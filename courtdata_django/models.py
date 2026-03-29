# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class Cases(models.Model):
    case_id = models.BigIntegerField(primary_key=True)
    case_number = models.CharField(blank=True, null=True)
    case_title = models.CharField(blank=True, null=True)
    case_description = models.TextField(blank=True, null=True)
    case_status = models.CharField(blank=True, null=True)
    case_filed_date = models.DateField(blank=True, null=True)
    created = models.DateTimeField(blank=True, null=True)
    created_dag_run_id = models.CharField(blank=True, null=True)
    updated = models.DateTimeField(blank=True, null=True)
    updated_task_id = models.CharField(blank=True, null=True)
    pipeline_status = models.CharField(default='New')

    class Meta:
        db_table = 'cases'


class Party(models.Model):
    party_id = models.BigIntegerField(primary_key=True)
    party_name = models.CharField(blank=True, null=True)
    party_type = models.CharField(blank=True, null=True)
    address = models.ForeignKey('Address', models.DO_NOTHING, blank=True, null=True)
    created = models.DateTimeField(blank=True, null=True)
    created_dag_run_id = models.CharField(blank=True, null=True)
    updated = models.DateTimeField(blank=True, null=True)
    updated_task_id = models.CharField(blank=True, null=True)

    class Meta:
        db_table = 'party'


class Attorney(models.Model):
    attorney_id = models.BigIntegerField(primary_key=True)
    attorney_name = models.CharField(blank=True, null=True)
    party = models.ForeignKey(Party, models.DO_NOTHING, blank=True, null=True)
    attorney_type = models.CharField(blank=True, null=True)
    address = models.ForeignKey('Address', models.DO_NOTHING, blank=True, null=True)
    created = models.DateTimeField(blank=True, null=True)
    created_dag_run_id = models.CharField(blank=True, null=True)
    updated = models.DateTimeField(blank=True, null=True)
    updated_task_id = models.CharField(blank=True, null=True)

    class Meta:
        db_table = 'attorney'


class Address(models.Model):
    address_id = models.BigIntegerField(primary_key=True)
    address_type = models.CharField(blank=True, null=True)
    address_line1 = models.CharField(blank=True, null=True)
    address_line2 = models.CharField(blank=True, null=True)
    city = models.CharField(blank=True, null=True)
    state = models.CharField(blank=True, null=True)
    country = models.CharField(blank=True, null=True)
    postal_code = models.CharField(blank=True, null=True)

    class Meta:
        db_table = 'address'


class Disposition(models.Model):
    disposition_id = models.BigIntegerField(primary_key=True)
    case = models.ForeignKey(Cases, models.DO_NOTHING, blank=True, null=True)
    disposition_status = models.CharField(blank=True, null=True)
    disposition_status_date = models.DateField(blank=True, null=True)
    disposition_code = models.CharField(blank=True, null=True)
    disposition_date = models.DateField(blank=True, null=True)
    judge = models.CharField(blank=True, null=True)
    created = models.DateTimeField(blank=True, null=True)
    created_dag_run_id = models.CharField(blank=True, null=True)
    updated = models.DateTimeField(blank=True, null=True)
    updated_task_id = models.CharField(blank=True, null=True)

    class Meta:
        db_table = 'disposition'


class Events(models.Model):
    event_id = models.BigIntegerField(primary_key=True)
    case = models.ForeignKey(Cases, models.DO_NOTHING, blank=True, null=True)
    event_name = models.CharField(blank=True, null=True)
    event_date = models.DateField(blank=True, null=True)
    event_start_time = models.CharField(blank=True, null=True)
    event_end_time = models.CharField(blank=True, null=True)
    event_judge = models.CharField(blank=True, null=True)
    event_courtroom = models.CharField(blank=True, null=True)
    event_result = models.CharField(blank=True, null=True)
    created = models.DateTimeField(blank=True, null=True)
    created_dag_run_id = models.CharField(blank=True, null=True)
    updated = models.DateTimeField(blank=True, null=True)
    updated_task_id = models.CharField(blank=True, null=True)

    class Meta:
        db_table = 'events'


class Docket(models.Model):
    docket_id = models.BigIntegerField(primary_key=True)
    case = models.ForeignKey(Cases, models.DO_NOTHING, blank=True, null=True)
    docket_date = models.DateField(blank=True, null=True)
    docket_text = models.TextField(blank=True, null=True)
    docket_currency = models.CharField(blank=True, null=True)
    docket_amount = models.DecimalField(max_digits=65535, decimal_places=65535, blank=True, null=True)
    docket_balance = models.DecimalField(max_digits=65535, decimal_places=65535, blank=True, null=True)
    created = models.DateTimeField(blank=True, null=True)
    created_dag_run_id = models.CharField(blank=True, null=True)
    updated = models.DateTimeField(blank=True, null=True)
    updated_task_id = models.CharField(blank=True, null=True)

    class Meta:
        db_table = 'docket'


class RawCases(models.Model):
    case_id = models.BigAutoField(primary_key=True)
    case_number = models.CharField()
    case_title = models.CharField(blank=True, null=True)
    case_description = models.TextField(blank=True, null=True)
    case_status = models.CharField(blank=True, null=True)
    case_filed_date = models.DateField(blank=True, null=True)
    created = models.DateTimeField(blank=True, null=True)
    created_by = models.CharField(blank=True, null=True)
    updated = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(blank=True, null=True)
    first_party_name = models.CharField(blank=True, null=True)
    first_party_type = models.CharField(blank=True, null=True)
    first_attorney_name = models.CharField(blank=True, null=True)
    first_attorney_type = models.CharField(blank=True, null=True)
    first_event_start = models.DateTimeField(blank=True, null=True)
    first_event_end = models.DateTimeField(blank=True, null=True)
    first_event_judge = models.CharField(blank=True, null=True)
    parties = models.JSONField(blank=True, null=True)
    attorneys = models.JSONField(blank=True, null=True)
    events = models.JSONField(blank=True, null=True)
    docket = models.JSONField(blank=True, null=True)
    dispositions = models.JSONField(blank=True, null=True)
    raw_html = models.TextField(blank=True, null=True)
    ingested_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = 'raw_cases'


class CaseParty(models.Model):
    case = models.OneToOneField(Cases, models.DO_NOTHING, primary_key=True)  # The composite primary key (case_id, party_id) found, that is not supported. The first column is selected.
    party = models.ForeignKey(Party, models.DO_NOTHING)

    class Meta:
        db_table = 'case_party'
        unique_together = (('case', 'party'),)
