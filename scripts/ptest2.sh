ulimit -c unlimited

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./command.sh 0 "pgbench -i -s 1"
./query.sh 1 "SELECT * FROM pgbench_accounts;"
./all-stop.sh $1
