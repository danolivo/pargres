ulimit -c unlimited
. ./paths.sh
cp contrib/pargres/scripts/* ./

# More JOINs in a query

./all-start.sh $1
./file.sh 0 "ptest8.sql"
./all-stop.sh $1
