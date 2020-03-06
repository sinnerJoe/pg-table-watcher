SELECT trigger_name as "triggerName"
      ,event_manipulation as "eventManipulation"
FROM  information_schema.triggers
WHERE event_object_table = ${table}
  AND event_object_schema = ${schema}