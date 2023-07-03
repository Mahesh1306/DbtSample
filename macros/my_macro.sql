{% macro UpdatebaseTable() %}
{%set temp = ref('ImportCommonRaw_ID') %}
{% for row in temp.ImportDataTransform5%}
update ImportCommonRaw set ProcessedDateTime = {{row.O_TIMESTAMP}},
RecordStatus = {{row.O_PROCESSED}}
where ID = row.id
{% endfor %}
{% endmacro %}