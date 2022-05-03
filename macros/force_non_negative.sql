{% macro force_non_negative(column_name) %}
    case 
        when {{ column_name }} < 0 or {{ column_name }} is NULL then 0
        else {{ column_name }}
    end
{% endmacro %}