-- ##############################################
-- Cast a column safely to a target type
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{% macro cast_safe(column_name, target_type) %}
    cast({{ column_name }} as {{ target_type }})
{% endmacro %}
