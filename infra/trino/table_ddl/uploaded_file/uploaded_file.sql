drop table iceberg.default.uploaded_file;
create table iceberg.default.uploaded_file(
	fileName varchar
)
WITH (
	location = 's3a://lake/uploaded/'
);