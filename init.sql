\echo Use "CREATE EXTENSION pargres" to load this file. \quit

CREATE TABLE IF NOT EXISTS @extschema@.relsfrag (
	relname		VARCHAR NOT NULL,
	attno		INT,
	fr_func_id	INT
);

