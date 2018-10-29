ulimit -c unlimited

cp contrib/pargres/scripts/* ./

./all-start.sh $1
./command.sh 0 "pgbench -i -s 1"
./query.sh 0 "INSERT INTO pgbench_accounts (SELECT * FROM pgbench_accounts) ON CONFLICT DO NOTHING;"
./query.sh 0 "DELETE FROM pgbench_accounts WHERE isLocalValue('pgbench_accounts', aid) = false;"
./query.sh 0 "INSERT INTO pgbench_tellers (SELECT * FROM pgbench_tellers) ON CONFLICT DO NOTHING;"
./query.sh 0 "DELETE FROM pgbench_tellers WHERE isLocalValue('pgbench_tellers', tid) = false;"
./query.sh 0 "INSERT INTO pgbench_branches (SELECT * FROM pgbench_branches) ON CONFLICT DO NOTHING;"
./query.sh 0 "DELETE FROM pgbench_branches WHERE isLocalValue('pgbench_branches', bid) = false;"
./query.sh 0 "VACUUM FULL;"
./query.sh 0 "explain select count(*) from pgbench_accounts;"
./query.sh 1 "explain select count(*) from pgbench_accounts;"

./query.sh 1 "select count(*) from pgbench_accounts;"
#./command.sh 0 "pgbench -t 1"
./all-stop.sh $1
