CREATE TRIGGER table_watcher_insert_trigger
AFTER INSERT
ON ${tableName:raw}
FOR EACH ROW
EXECUTE PROCEDURE table_watcher_notify_insert(${tableName});