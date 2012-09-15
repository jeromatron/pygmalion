------
-- A simple way to duplicate a column family
------

-- Don't forget to create the new column family first
rows = LOAD 'cassandra://pygmalion/account' USING CassandraStorage();
STORE rows INTO 'cassandra://pygmalion/account_copy' USING CassandraStorage();