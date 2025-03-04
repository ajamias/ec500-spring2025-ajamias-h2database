-- Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table test(a int primary key, b int references(a));
> ok

merge into test values(1, 2);
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

EXPLAIN SELECT * FROM TEST WHERE ID=1;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ WHERE "ID" = 1

EXPLAIN MERGE INTO TEST VALUES(1, 'Hello');
>> MERGE INTO "PUBLIC"."TEST"("ID", "NAME") KEY("ID") VALUES (1, 'Hello')

MERGE INTO TEST VALUES(1, 'Hello');
> update count: 1

MERGE INTO TEST VALUES(1, 'Hi');
> update count: 1

MERGE INTO TEST VALUES(2, 'World');
> update count: 1

MERGE INTO TEST VALUES(2, 'World!');
> update count: 1

MERGE INTO TEST(ID, NAME) VALUES(3, 'How are you');
> update count: 1

EXPLAIN MERGE INTO TEST(ID, NAME) VALUES(3, 'How are you');
>> MERGE INTO "PUBLIC"."TEST"("ID", "NAME") KEY("ID") VALUES (3, 'How are you')

MERGE INTO TEST(ID, NAME) KEY(ID) VALUES(3, 'How do you do');
> update count: 1

EXPLAIN MERGE INTO TEST(ID, NAME) KEY(ID) VALUES(3, 'How do you do');
>> MERGE INTO "PUBLIC"."TEST"("ID", "NAME") KEY("ID") VALUES (3, 'How do you do')

MERGE INTO TEST(ID, NAME) KEY(NAME) VALUES(3, 'Fine');
> exception DUPLICATE_KEY_1

MERGE INTO TEST(ID, NAME) KEY(NAME) VALUES(4, 'Fine!');
> update count: 1

MERGE INTO TEST(ID, NAME) KEY(NAME) VALUES(4, 'Fine! And you');
> exception DUPLICATE_KEY_1

MERGE INTO TEST(ID, NAME) KEY(NAME, ID) VALUES(5, 'I''m ok');
> update count: 1

MERGE INTO TEST(ID, NAME) KEY(NAME, ID) VALUES(5, 'Oh, fine');
> exception DUPLICATE_KEY_1

MERGE INTO TEST(ID, NAME) VALUES(6, 'Oh, fine.');
> update count: 1

SELECT * FROM TEST;
> ID NAME
> -- -------------
> 1  Hi
> 2  World!
> 3  How do you do
> 4  Fine!
> 5  I'm ok
> 6  Oh, fine.
> rows: 6

MERGE INTO TEST SELECT ID+4, NAME FROM TEST;
> update count: 6

SELECT * FROM TEST;
> ID NAME
> -- -------------
> 1  Hi
> 10 Oh, fine.
> 2  World!
> 3  How do you do
> 4  Fine!
> 5  Hi
> 6  World!
> 7  How do you do
> 8  Fine!
> 9  I'm ok
> rows: 10

DROP TABLE TEST;
> ok

-- Test for the index matching logic in org.h2.command.dml.Merge

CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE1 INT, VALUE2 INT, UNIQUE(VALUE1, VALUE2));
> ok

MERGE INTO TEST KEY (ID) VALUES (1, 2, 3), (2, 2, 3);
> exception DUPLICATE_KEY_1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A INT, B INT DEFAULT 5);
> ok

MERGE INTO TEST KEY(A) VALUES (1, DEFAULT);
> update count: 1

TABLE TEST;
> A B
> - -
> 1 5
> rows: 1

UPDATE TEST SET B = 1 WHERE A = 1;
> update count: 1

SELECT B FROM TEST WHERE A = 1;
>> 1

MERGE INTO TEST KEY(A) VALUES (1, DEFAULT);
> update count: 1

SELECT B FROM TEST WHERE A = 1;
>> 5

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A INT, B INT GENERATED ALWAYS AS (A + 1));
> ok

MERGE INTO TEST KEY(A) VALUES (1, 1);
> exception GENERATED_COLUMN_CANNOT_BE_ASSIGNED_1

MERGE INTO TEST KEY(A) VALUES (1, DEFAULT);
> update count: 1

MERGE INTO TEST KEY(A) VALUES (1, 1);
> exception GENERATED_COLUMN_CANNOT_BE_ASSIGNED_1

MERGE INTO TEST KEY(A) VALUES (1, DEFAULT);
> update count: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT, G INT GENERATED ALWAYS AS (ID + 1));
> ok

MERGE INTO TEST(G) KEY(ID) VALUES (1);
> exception SYNTAX_ERROR_2

DROP TABLE TEST;
> ok

CREATE TABLE T(ID BOOLEAN PRIMARY KEY);
> ok

INSERT INTO T(ID) VALUES (TRUE);
> update count: 1

MERGE INTO T(ID) VALUES 2;
> exception TYPES_ARE_NOT_COMPARABLE_2

SET MODE MySQL;
> ok

MERGE INTO T(ID) VALUES 2;
> update count: 1

SET MODE Regular;
> ok

DROP TABLE T;
> ok
