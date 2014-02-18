CREATE OR REPLACE FUNCTION create_audit(enable_replica BOOLEAN)
RETURNS SETOF text
IMMUTABLE STRICT
LANGUAGE SQL
AS $audit$
SELECT format('CREATE SCHEMA IF NOT EXISTS %I;', nspname || '_audit')
FROM pg_catalog.pg_namespace n
WHERE
    n.nspname NOT IN ('pg_catalog','information_schema') AND /* Leave out the stuff in the catalog */
    n.nspname !~ '(^(pg|)_|_audit$)'                         /* Also omit anything that looks like PostgreSQL, Slony or Audit owns it. */
UNION ALL
(
    WITH t0 AS (
        SELECT
            n.nspname AS "schema",
            c.relname AS "table",
            a.attname AS "column_name",
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS "column_type"
        FROM
            pg_catalog.pg_attribute a
        JOIN
            pg_catalog.pg_class c
            ON (
                c.relkind = 'r' AND
                c.oid = a.attrelid
            )
        JOIN
            pg_catalog.pg_namespace n
            ON (
                c.relnamespace = n.oid AND
                n.nspname NOT IN ('pg_catalog','information_schema') AND /* Leave out the stuff in the catalog */
                n.nspname !~ '(^(pg|)_|_audit$)'                         /* Also omit anything that looks like PostgreSQL, Slony or Audit owns it. */
            )
        WHERE
            a.attnum > 0 AND
            NOT a.attisdropped
        ORDER BY c.relname, a.attnum
    )
    SELECT
        format(
            $q$CREATE TABLE IF NOT EXISTS %I.%I (
    %s,
    stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
    "current_user" TEXT NOT NULL DEFAULT current_user,
    "session_user" TEXT NOT NULL DEFAULT session_user,
    operation TEXT NOT NULL
);

CREATE OR REPLACE FUNCTION %I.%I()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_old %I.%I%%ROWTYPE;
    v_new %I.%I%%ROWTYPE;
    v_ret %I.%I%%ROWTYPE;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        v_old := OLD;
        v_new := NULL;
        v_ret := OLD;
    ELSIF (TG_OP = 'INSERT') THEN
        v_old := NULL;
        v_new := NEW;
        v_ret := NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
        v_old := OLD;
        v_new := NEW;
        v_ret := NEW;
    END IF;
    INSERT INTO %I.%I (
        %s,
        %s,
        operation
    )
    VALUES((v_old).*, (v_new).*, TG_OP);
    RETURN v_ret;
END;
$$;

CREATE TRIGGER %I
AFTER INSERT OR UPDATE OR DELETE ON %I.%I
    FOR EACH ROW EXECUTE PROCEDURE %I.%I();
%s
INSERT INTO %I.%I (
    %s,
    operation
)
SELECT
    %s,
    'INSERT'
FROM %I.%I;

$q$,
        "schema" || '_audit', "table",
        string_agg(
            quote_ident("column_name" || '_old') || ' ' || column_type || E',\n    ' ||
            quote_ident("column_name" || '_new') || ' ' || column_type,   E',\n    '
        ),
        "schema" || '_audit', "table",
        "schema", "table",
        "schema", "table",
        "schema", "table",
        "schema" || '_audit', "table",
        string_agg(quote_ident("column_name" || '_old'), E',\n        '),
        string_agg(quote_ident("column_name" || '_new'), E',\n        '),
        quote_ident("schema" || '_' || "table" || '_audit'),
        "schema", "table",
        "schema" || '_audit', "table",
        CASE
        WHEN enable_replica THEN
            format(
                '%sALTER TABLE %I.%I ENABLE REPLICA TRIGGER %I;%s',
                E'\n', "schema", "table", "schema" || '_' || "table" || '_audit', E'\n'
            )
        ELSE ''
        END,
        "schema" || '_audit', "table",
        string_agg(quote_ident("column_name" || '_new'), E',\n    '),
        string_agg(quote_ident("column_name"), E',\n    '),
        "schema", "table"
        ) AS "table and trigger"
        FROM
            t0
        GROUP BY "schema", "table"
)
UNION ALL
/* Indexes for each unique key */
(
    WITH t1 AS (
        SELECT
            n.nspname::text,
            c.relname::text,
            array_agg(a.attname::text ORDER BY k.ord) AS "cols"
        FROM
            pg_catalog.pg_class c
        JOIN
            pg_catalog.pg_namespace n
            ON (
                c.relkind = 'r' AND
                c.relnamespace = n.oid AND
                n.nspname NOT IN ('pg_catalog','information_schema') AND
                n.nspname !~ '(^(pg|)_|_audit$)'
            )
        JOIN
            pg_catalog.pg_constraint co
            ON (
                c.oid = co.conrelid AND
                co.contype IN ('p','u')
            )
        CROSS JOIN LATERAL
            /*
             * XXX In 9.4+, replace the hack below with
             * UNNEST(co.conkey) WITH ORDINALITY AS k(col, ord)
             */
            (SELECT col, row_number() OVER () AS ord FROM UNNEST(co.conkey) AS u(col)) AS k
        JOIN
            pg_catalog.pg_attribute a
            ON (
                k.col = a.attnum AND
                c.oid = a.attrelid
            )
        GROUP BY n.nspname, c.relname, co.conname
    )
    SELECT
        format(
            'CREATE INDEX ON %I.%I (%s);',
            nspname || '_audit',
            relname,
            (SELECT string_agg(u || '_' || v, ', ') FROM UNNEST(cols) AS u(u))
        )
    FROM
        t1
    CROSS JOIN
        (VALUES('old'),('new')) AS o_n(v)
);
$audit$;

CREATE OR REPLACE FUNCTION create_audit()
RETURNS SETOF TEXT
LANGUAGE SQL
AS $$
SELECT create_audit(false);
$$;

/* XXX Need to be able to handle adding columns.  Prolly a catalog
 * lookup.  Does 9.3 have DDL triggers we can use? */
