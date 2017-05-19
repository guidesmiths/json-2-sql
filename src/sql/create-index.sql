do
$$
begin
if not exists (
    select indexname
        from pg_indexes
    where schemaname = %L:schema
        and tablename = %L:table
        and indexname = %L:indexName
)
then
    create index %I:indexName ON %I:schema.%I:table(%I:columns);
end if;
end
$$;
