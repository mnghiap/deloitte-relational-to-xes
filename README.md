# deloitte-relational-to-xes

### Introduction

The script `relational_to_xes.py` extracts automatically an IEEE XES event log (please see https://xes-standard.org/) from a relational database using PM4Py 2.2.24, given that:
- The database has a Python connector compliant with Python DB API 2.0. Currently supported DBs are PostgreSQL and SQLite3.
- The database schema is known beforehand and there is no cycle in this schema.

To execute this script, change the following inputs in the code:
- `db_access_params`: Parameters used to access the database
- `annotations`: A list of annotations in the schema. Interface for an annotation is Annotation(table1, key1, table2, key2, annot_type),\
which means atribute `key1` of `table1` is associated with `key2` of `table2` with the annotation type `annot_type`. `annot_type` can be one of `AnnotationType.{ONE_TO_ONE, ONE_TO_MANY, MANY_TO_MANY}`.
- `converter`: Setting up a `Converter` object. Parameters are the event table, the case table, activity key, timestamp key, case ID key, and the `annotations`.
- `converter.set_database_adapter(DB_NAME)`: Replace `DB_NAME` with the name or the connector of the database.
- In `build_event_log`:
  - Set the desired exported log name.
  - Set the `event_query`. This is a string to define how to extract different acitivity key for each event. General syntax is `if {python_expression} then {python_expression} (else if {python_expression})* else {python_expression}`.
  - Set the default activity, case ID, timestamp if no such attribute is found while extracting the log.

The script `extractor.py` is an old version that is not robust against events associated with multiple objects.
<br>
The other scripts are suggestions from [@Javert899](https://github.com/Javert899)
