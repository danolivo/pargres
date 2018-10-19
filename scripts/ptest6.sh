ulimit -c unlimited

. ./paths.sh

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./file.sh 0 "ptest6.sql"
./all-stop.sh $1
