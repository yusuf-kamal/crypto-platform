{#
  Constraints for dbt-managed tables.

  dbt owns gold.dim_symbol and the three fact tables: on `--full-refresh` it
  drops and recreates them, which silently throws away any primary key,
  foreign key or check constraint a DDL script had added. That is exactly
  what used to happen here -- sql/01 created dim_symbol and fact_candles_1m
  with keys, dbt then owned the same names, and the first full refresh left
  bare tables behind. Declaring the constraints as hooks instead means they
  are restored after every build, full refresh included.

  Every macro is a no-op on a table that already has the constraint, so a
  normal incremental run changes nothing.
#}

{% macro not_null_columns(columns) %}
  {#-
    A CTAS makes every column nullable, and a key column cannot be. The
    guard is not optional: once the primary key exists, SQL Server rejects
    even a no-op ALTER COLUMN on an indexed column (error 5074), so this has
    to do nothing on every run after the first.
  -#}
  {% for column in columns %}
    if exists (
        select 1 from sys.columns
        where object_id = object_id('{{ this }}')
          and name = '{{ column.name }}'
          and is_nullable = 1
    )
        alter table {{ this }}
            alter column {{ column.name }} {{ column.type }} not null;
  {% endfor %}
{% endmacro %}


{% macro add_primary_key(constraint_name, columns) %}
  if not exists (
      select 1 from sys.key_constraints
      where name = '{{ constraint_name }}'
        and parent_object_id = object_id('{{ this }}')
  )
      alter table {{ this }}
          add constraint {{ constraint_name }} primary key ({{ columns }});
{% endmacro %}


{% macro add_foreign_key(constraint_name, columns, references) %}
  if not exists (
      select 1 from sys.foreign_keys
      where name = '{{ constraint_name }}'
        and parent_object_id = object_id('{{ this }}')
  )
      alter table {{ this }}
          add constraint {{ constraint_name }} foreign key ({{ columns }})
          references {{ references }};
{% endmacro %}


{% macro add_check(constraint_name, expression) %}
  if not exists (
      select 1 from sys.check_constraints
      where name = '{{ constraint_name }}'
        and parent_object_id = object_id('{{ this }}')
  )
      alter table {{ this }}
          add constraint {{ constraint_name }} check ({{ expression }});
{% endmacro %}


{% macro drop_foreign_keys_referencing(schema_name, table_name) %}
  {#
    A table cannot be dropped while a foreign key points at it, so a
    `--full-refresh` of dim_symbol would fail once the facts reference it.
    This runs as dim_symbol's pre-hook; the facts re-add their keys in their
    own post-hooks a moment later, because dbt always builds dim_symbol
    first (both facts `ref()` it).
  #}
  declare @drop_fks nvarchar(max) = N'';
  select @drop_fks = @drop_fks
       + N'alter table ' + quotename(schema_name(child.schema_id))
       + N'.' + quotename(child.name)
       + N' drop constraint ' + quotename(fk.name) + N';'
  from sys.foreign_keys fk
  join sys.tables child on child.object_id = fk.parent_object_id
  where fk.referenced_object_id = object_id('{{ schema_name }}.{{ table_name }}');
  if @drop_fks <> N'' exec sp_executesql @drop_fks;
{% endmacro %}
