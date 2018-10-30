# Main goals: Nested loop quals & rescan node

ulimit -c unlimited

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./all-pause.sh $1
./query.sh 0 "CREATE TABLE test (a serial, b int);"
./query.sh 0 "INSERT INTO test (b) values (1);"
./query.sh 0 "INSERT INTO test (b) values (4);"
./query.sh 1 "INSERT INTO test (b) values (2);"
./query.sh 1 "INSERT INTO test (b) values (3);"
./query.sh 0 "CREATE TABLE joined (x int);"
./query.sh 0 "INSERT INTO joined (x) values (1);"
./query.sh 0 "INSERT INTO joined (x) values (10);"
./query.sh 0 "INSERT INTO joined (x) values (100);"
./query.sh 0 "SELECT * FROM test, joined WHERE a <> x;"
./all-stop.sh $1
