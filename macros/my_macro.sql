{% macro UpdatebaseTable(ImportDataTransform5) %}
{% for row in ImportDataTransform5%}
{{breakpoint()}}
update ImportCommonRaw set ProcessedDateTime = GETDATE(),
RecordStatus = {{row.O_PROCESSED}}
{% endfor %}
{% endmacro %}