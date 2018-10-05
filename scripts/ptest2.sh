ulimit -c unlimited

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./all-command.sh $1 "pgbench -i -s 1"
./all-query.sh $1 "SELECT count(*) FROM pgbench_accounts;"
#./all-stop.sh $1
