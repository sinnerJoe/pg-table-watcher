CREATE TRIGGER table_watcher_update_trigger
AFTER UPDATE
ON ${tableName:raw}
FOR EACH ROW
EXECUTE PROCEDURE table_watcher_notify_update(${tableName});