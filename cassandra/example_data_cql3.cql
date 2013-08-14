CREATE KEYSPACE pygmalion_cql3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use pygmalion_cql3;

create table account_cql3 (
	last_name text PRIMARY KEY,
	first_name text,
	birth_place text,
	num_heads int,
	my_time_uuid timeuuid);

insert into account_cql3 (last_name, first_name, birth_place, num_heads) values ('Beeblebrox', 'Zaphod', 'Betelgeuse Five', 2);
insert into account_cql3 (last_name, first_name, birth_place, num_heads) values ('Prefect', 'Ford', 'Betelgeuse Five', 1);
insert into account_cql3 (last_name, first_name, birth_place, num_heads) values ('Dent', 'Arthur', 'Earth', 1);
