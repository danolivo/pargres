ulimit -c unlimited

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./command.sh 0 "pgbench -i -s 1"
./query.sh 0 "INSERT INTO pgbench_accounts (SELECT * FROM pgbench_accounts) ON CONFLICT DO NOTHING;"
#./query.sh 1 "SELECT count(*) FROM pgbench_accounts;"
./query.sh 1 "DELETE FROM pgbench_accounts WHERE isLocalValue('pgbench_accounts', aid) = false;"
#./query.sh 1 "VACUUM verbose pgbench_accounts;"
./query.sh 1 "explain SELECT count(*) FROM pgbench_accounts;"
./query.sh 1 "SELECT count(*) FROM pgbench_accounts;"
./all-stop.sh $1
