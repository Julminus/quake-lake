{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
-- Rows whose value is outside [min_value, max_value]; nulls are not_null's job.
select {{ column_name }}
from {{ model }}
where
    {% if min_value is not none %}{{ column_name }} < {{ min_value }}{% else %}false{% endif %}
    or {% if max_value is not none %}{{ column_name }} > {{ max_value }}{% else %}false{% endif %}
{% endtest %}
