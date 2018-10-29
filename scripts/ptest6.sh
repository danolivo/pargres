ulimit -c unlimited

. ./paths.sh

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./file.sh 1 "ptest6.sql"
./file.sh 0 "ptest6_1.sql"
./file.sh 1 "ptest6_1.sql"
./all-stop.sh $1
