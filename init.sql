\echo Use "CREATE EXTENSION pargres" to load this file. \quit

CREATE TABLE IF NOT EXISTS @extschema@.relsfrag (
	relname		VARCHAR NOT NULL,
	attno		INT,
	fr_func_id	INT
);

--
-- set_query_id()
--
CREATE FUNCTION set_query_id(
					nodeID	INT, 
					tag		INT)
RETURNS VOID
AS 'MODULE_PATHNAME', 'set_query_id'
LANGUAGE C STRICT PARALLEL RESTRICTED;

