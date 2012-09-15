register 'pygmalion.jar';
register 'hector-core-0.7.0-28.jar';
register 'uuid-3.2.0.jar';

define FromCassandraBag org.pygmalion.udf.FromCassandraBag();
define ToCassandraBag org.pygmalion.udf.ToCassandraBag();
define GenerateTimeUUID org.pygmalion.udf.uuid.GenerateTimeUUID();

raw_account = LOAD 'cassandra://pygmalion/account' USING CassandraStorage() AS (key:chararray, columns:bag {column:tuple (name, value)});

account = FOREACH raw_account GENERATE
                    key AS key,
                    FLATTEN(FromCassandraBag('first_name, last_name',columns)) AS (
                        first_name: chararray,
                        last_name: chararray
                );

account_w_timeuuid = FOREACH account GENERATE
  key AS key,
  first_name AS first_name,
  last_name AS last_name,
  GenerateTimeUUID() AS my_time_uuid;
  
account_cassandra = FOREACH account_w_timeuuid GENERATE FLATTEN(ToCassandraBag(key, first_name, last_name, my_time_uuid)) AS (
  key: chararray,
  first_name: chararray,
  last_name: chararray,
  my_time_uuid: chararray
);

STORE account_cassandra INTO 'cassandra://pygmalion/account' USING CassandraStorage();