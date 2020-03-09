SELECT table_schema as "schema", table_name as "name"
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'