\echo Use "CREATE EXTENSION pargres" to load this file. \quit

CREATE TABLE IF NOT EXISTS @extschema@.relsfrag (
	relname		VARCHAR NOT NULL,
	attno		INT,
	fr_func_id	INT
);

--
-- set_query_id()
--
CREATE OR REPLACE FUNCTION @extschema@.set_query_id(
					nodeID	INT)
RETURNS INT
AS 'MODULE_PATHNAME', 'set_query_id'
LANGUAGE C STRICT;

--
-- set_exchange_ports()
--
CREATE OR REPLACE FUNCTION @extschema@.set_exchange_ports(
					ports	TEXT)
RETURNS VOID
AS 'MODULE_PATHNAME', 'set_exchange_ports'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.isLocalValue(
					relname	TEXT,
					value	INT)
RETURNS BOOL
AS 'MODULE_PATHNAME', 'isLocalValue'
LANGUAGE C STRICT;
