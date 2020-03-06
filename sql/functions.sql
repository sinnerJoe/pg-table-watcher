CREATE OR REPLACE FUNCTION table_watcher_notify_delete ()
RETURNS TRIGGER
 LANGUAGE plpgsql
AS $$
DECLARE
  tablename text := TG_ARGV[0];
BEGIN
  PERFORM pg_notify(${deleteChannel}, (
  	SELECT row_to_json(payload) FROM 
  		(SELECT row_to_json(OLD) AS "before", 
							tablename AS "table") payload)::TEXT );
	RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION table_watcher_notify_update ()
RETURNS TRIGGER
 LANGUAGE plpgsql
AS $$
DECLARE
  tablename text := TG_ARGV[0];
BEGIN
  PERFORM pg_notify(${updateChannel}, (
  	SELECT row_to_json(payload) FROM 
  		(SELECT row_to_json(OLD) AS "before",
						  row_to_json(NEW) AS "after", 
							tablename AS "table") payload)::TEXT );
		RETURN NULL;
END;
$$;


CREATE OR REPLACE FUNCTION table_watcher_notify_insert ()
RETURNS TRIGGER
 LANGUAGE plpgsql
AS $$
DECLARE
  tablename text := TG_ARGV[0];
BEGIN
  PERFORM pg_notify(${insertChannel}, (
  	SELECT row_to_json(payload) FROM 
  		(SELECT row_to_json(NEW) AS "after", 
							tablename AS "table") payload)::TEXT );
	RETURN NULL;
END;
$$;