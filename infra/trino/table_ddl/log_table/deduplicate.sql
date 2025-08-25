DELETE FROM iceberg.default.log_table
        WHERE (filename, updated_date) NOT IN (
            SELECT filename, MAX(updated_date)
            FROM iceberg.default.log_table
            GROUP BY filename