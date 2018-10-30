ulimit -c unlimited

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./all-stop.sh $1
