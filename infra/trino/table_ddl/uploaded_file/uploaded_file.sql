create table iceberg.default.uploaded_file(
	fileName varchar,
	fileHash varchar
)
WITH (
	location = 's3a://lake/uploaded/'
);