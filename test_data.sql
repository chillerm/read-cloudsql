USE test;

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    column_1 INT ,
    column_2 VARCHAR(500),
    last_updated TIMESTAMP
);

INSERT INTO test (column_1, column_2, last_updated)
 VALUES (1, 'One', CURRENT_TIMESTAMP() ),
        (2, 'Two', CURRENT_TIMESTAMP() ),
        (3, 'Three', CURRENT_TIMESTAMP() )
;