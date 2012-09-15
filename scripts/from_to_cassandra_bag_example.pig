register 'pygmalion.jar';

define FromCassandraBag org.pygmalion.udf.FromCassandraBag();
define ToCassandraBag org.pygmalion.udf.ToCassandraBag();

raw =  LOAD 'cassandra://pygmalion/account' USING CassandraStorage();
rows = FOREACH raw GENERATE key, FLATTEN(FromCassandraBag('first_name, last_name, birth_place', columns)) AS (
    first_name:chararray,
    last_name:chararray,
    birth_place:chararray
);

betelgeuse_born = FILTER rows BY (birth_place matches '.*[Bb]etelgeuse.*');

betelgeuse_cassandra = FOREACH betelgeuse_born GENERATE
    FLATTEN(ToCassandraBag(first_name, last_name, birth_place));

STORE betelgeuse_cassandra INTO 'cassandra://pygmalion/betelgeuse' USING CassandraStorage();
