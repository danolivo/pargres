CREATE TABLE A1 (
  id1	Serial,
  a1	INT,
  PRIMARY KEY (id1)
);

CREATE TABLE A2 (
  id2	Serial,
  a2	INT,
  b2	INT,
  PRIMARY KEY (id2)
);

CREATE TABLE A3 (
  id3	Serial,
  b3	INT,
  PRIMARY KEY (id3)
);

INSERT INTO A1 (a1) VALUES (2) ,(3), (4), (5), (6);
INSERT INTO A2 (a2, b2) VALUES (2, 7) ,(2, 8), (3, 3), (4, 4), (5, 5);
INSERT INTO A3 (b3) VALUES (7) ,(8), (9), (10), (11);

explain SELECT * FROM A1, A2, A3 WHERE (a1 = a2) AND (b2 = b3);
SELECT A1.a1, A2.b2, A1.id1 FROM A1, A2, A3 WHERE (a1 = a2) AND (b2 = b3);

