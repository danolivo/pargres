ulimit -c unlimited

cp contrib/pargres/scripts/* ./

./all-start.sh $1
ps -ef | grep postgre
./all-query.sh $1 "SELECT * FROM relsfrag;"
./all-query.sh $1 "CREATE TABLE test (a serial, b int);"
./all-query.sh $1 "INSERT INTO test (b) values (1);"
./all-query.sh $1 "explain SELECT * FROM test;"
./all-query.sh $1 "SELECT * FROM test;"
./all-query.sh $1 "explain INSERT INTO test (b) values (1);"
./all-query.sh $1 "SELECT * FROM relsfrag;"
./all-stop.sh $1
