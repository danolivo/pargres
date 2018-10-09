ulimit -c unlimited

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./file.sh 0 "sql.sql"
./all-stop.sh $1
