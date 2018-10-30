SELECT DISTINCT CONCAT('(', LEAST(g1.id, g2.id), ',', GREATEST(g1.id, g2.id), ')')
FROM ya_goods g1 
INNER JOIN ya_goods g2 ON g1.num = g2.num 
WHERE g1.id <> g2.id;

