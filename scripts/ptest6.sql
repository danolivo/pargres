CREATE TABLE ya_goods (
  id Serial NOT NULL,
  num	INT NOT NULL, 
  name varchar(64) NOT NULL,
  PRIMARY KEY (id)
);
insert into ya_goods (id, num, name) values (1, 1, 'яблоки'), (2, 1, 'яблоки') ,(3, 3, 'груши'), (4, 1, 'яблоки'), (5, 5, 'апельсины'), (6, 3, 'груши');

select * from ya_goods g1 JOIN ya_goods g2 ON g1.id = g2.num;

