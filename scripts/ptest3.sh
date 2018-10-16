ulimit -c unlimited

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./all-pause.sh $1
./query.sh 0 "CREATE TABLE test (a serial, b int);"
./query.sh 0 "INSERT INTO test (b) values (1);"
./query.sh 0 "INSERT INTO test (b) values (4);"
./query.sh 1 "INSERT INTO test (b) values (2);"
./query.sh 1 "INSERT INTO test (b) values (3);"
#./query.sh 1 "explain INSERT INTO test (b) values (4);"
#./query.sh 1 "explain SELECT * FROM test;"
#./query.sh 0 "SELECT * FROM test;"
./all-query.sh $1 "SELECT * FROM test;"
./all-stop.sh $1
