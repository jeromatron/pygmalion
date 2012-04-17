register 'pygmalion.jar';

define FromCassandraBag org.pygmalion.udf.FromCassandraBag();
define DeleteColumns org.pygmalion.udf.DeleteColumns();

raw = LOAD 'cassandra://SocialData/signal' USING org.apache.cassandra.hadoop.pig.CassandraStorage() AS (key:chararray, columns:bag {column:tuple (name, value)});

account = FOREACH raw_signals GENERATE key, FLATTEN(FromCassandraBag('first_name, last_name, birth_place',columns)) AS (
            first_name:     chararray,
            last_name:      chararray,
            birth_place:    chararray,
            num_heads:      long
          );

account_cassandra = FOREACH filtered GENERATE FLATTEN(DeleteColumns(key, num_heads));

STORE account_cassandra INTO 'cassandra://pygmalion/account' USING CassandraStorage();
