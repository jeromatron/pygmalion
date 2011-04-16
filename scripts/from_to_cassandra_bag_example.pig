register 'pygmalion.jar';

raw =  LOAD 'cassandra://pygmalion/account' USING CassandraStorage() AS (key:chararray, columns:bag {column:tuple (name, value)});
rows = FOREACH raw GENERATE key, FLATTEN(org.pygmalion.udf.FromCassandraBag('first_name, last_name, birth_place', columns)) AS (
    first_name:chararray,
    last_name:chararray,
    birth_place:chararray
);

betelgeuse_born = FILTER rows BY (birth_place matches '.*[Bb]etelgeuse.*');

betelguese_cassandra = FOREACH betelgeuse_born GENERATE
    FLATTEN(org.pygmalion.udf.ToCassandraBag(first_name, last_name, birth_place)) AS (
        first_name:chararray,
        last_name:chararray,
        birth_place:chararray
    );

STORE betelguese_cassandra INTO 'cassandra://pygmalion/betelgeuse' USING CassandraStorage();