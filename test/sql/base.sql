BEGIN;

CREATE EXTENSION pg_audit;

CREATE TABLE foo(id SERIAL, t TEXT);

INSERT INTO foo(t) VALUES ('One love');

SELECT create_audit();

ROLLBACK;
