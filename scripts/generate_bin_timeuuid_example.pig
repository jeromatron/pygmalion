/*
 * Example of using GenerateBinTimeUUID() UDF to generate UUIDs in binary 
 * format and store them into a TimeUUID column in Cassandra.
 *
 * NOTE: This example is using Cassandra 1.2 and CQL3 with CqlStorage()!
 */
 
register uuid-3.2.0.jar;
register hector-core-0.7.0-28.jar;
register pygmalion.jar;

define FromCassandraBag org.pygmalion.udf.FromCassandraBag();
define ToCassandraBag org.pygmalion.udf.ToCassandraBag();
define GenerateBinTimeUUID org.pygmalion.udf.uuid.GenerateBinTimeUUID();

raw_account = LOAD 'cql://pygmalion_cql3/account_cql3' USING CqlStorage();

account_w_timeuuid = FOREACH raw_account GENERATE
  TOTUPLE(TOTUPLE('last_name', $0.$1)),
  TOTUPLE(GenerateBinTimeUUID());

STORE account_w_timeuuid INTO 'cql://pygmalion_cql3/account_cql3?output_query=update account_cql3 set my_time_uuid@#' USING CqlStorage();
