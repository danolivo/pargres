CREATE TABLE ya_goods (
  id Serial NOT NULL,
  name varchar(64) NOT NULL,
  PRIMARY KEY (id)
);
insert into ya_goods values (1, 'яблоки'), (2, 'яблоки') ,(3, 'груши'), (4,'яблоки'), (5, 'апельсины'), (6, 'груши');
select * from ya_goods;

explain SELECT DISTINCT CONCAT('(', LEAST(g1.id, g2.id), ',', GREATEST(g1.id, g2.id), ')')
FROM ya_goods g1 
INNER JOIN ya_goods g2 ON g1.name = g2.name 
WHERE g1.id <> g2.id;

SELECT * FROM ya_goods g1 
INNER JOIN ya_goods g2 ON g1.name = g2.name;

