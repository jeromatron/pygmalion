register 'pygmalion.jar';

define FromCassandraBag org.pygmalion.udf.FromCassandraBag();

raw =  LOAD 'cassandra://pygmalion/updates' USING CassandraStorage();
rows = FOREACH raw GENERATE key, FLATTEN(FromCassandraBag('2012-Sep-14*', columns)) as updates;
count = foreach rows generate key, COUNT(updates);
dump count;