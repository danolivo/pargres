begin;
CREATE TABLE test (a serial, b int);
INSERT INTO test (b) values (1);
INSERT INTO test (b) values (2);
INSERT INTO test (b) values (3);
SELECT * FROM test;
end;
