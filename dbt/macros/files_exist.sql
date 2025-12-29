{% macro files_exist(path) %}
  {% set result = run_query(
      "select count(*) from glob(path)"
  ) %}
  {% if result and result.rows[0][0] > 0 %}
    {{ return(true) }}
  {% else %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}
