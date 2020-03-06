CREATE TRIGGER table_watcher_delete_trigger
AFTER DELETE
ON ${tableName:raw}
FOR EACH ROW
EXECUTE PROCEDURE table_watcher_notify_delete(${tableName});