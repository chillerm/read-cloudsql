USE test;

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    column_1 int ,
    column_2 varchar(500)
);

INSERT INTO test (column_1, column_2)
 VALUES (1, 'One'),
        (2, 'Two'),
        (3, 'Three')
;