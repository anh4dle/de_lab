drop table iceberg.default.log_table;
create table iceberg.default.log_table(
	fileName varchar,
	downloadURL varchar,
	status varchar,
	updated_date timestamp,
	error varchar
)
WITH (
	location = 's3a://lake/uploaded/'
);