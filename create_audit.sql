WITH t AS (
    SELECT
        quote_ident(n.nspname) AS "schema",
        c.relname || '_audit' AS "table",
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
        $q$CREATE TABLE %I.%I (
    %s,
    stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
    "current_user" TEXT NOT NULL,
    "session_user" TEXT NOT NULL,
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
        "current_user",
        "session_user",
        operation
    )
    VALUES((v_old).*, (v_new).*, current_user, session_user, TG_OP);
    RETURN v_ret;
END;
$$;

CREATE TRIGGER %I
AFTER INSERT OR UPDATE OR DELETE ON %I.%I
    FOR EACH ROW EXECUTE PROCEDURE %I.%I();$q$,
        "schema", "table",
        string_agg(
            quote_ident("column_name" || '_old') || ' ' || column_type || E',\n    ' ||
            quote_ident("column_name" || '_new') || ' ' || column_type,   E',\n    '
        ),
        "schema", "table",
        "schema", regexp_replace("table", '_audit$', ''),
        "schema", regexp_replace("table", '_audit$', ''),
        "schema", regexp_replace("table", '_audit$', ''),
        "schema", "table",
        string_agg(quote_ident("column_name" || '_old'), E',\n        '),
        string_agg(quote_ident("column_name" || '_new'), E',\n        '),
        quote_ident("schema" || '_' || "table"),
        "schema", regexp_replace("table", '_audit$', ''),
        "schema", "table"
    ) AS "table and trigger"
FROM
    t
GROUP BY "schema", "table"
;
/* XXX Need to be able to handle adding columns.  Prolly a catalog
 * lookup.  Does 9.3 have DDL triggers we can use? */
