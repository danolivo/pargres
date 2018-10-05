ulimit -c unlimited

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./all-query.sh $1 "CREATE TABLE test (a serial, b int);"
./all-query.sh $1 "explain INSERT INTO test (b) values (1);"
./all-stop.sh $1
