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
            n.nspname NOT IN ('pg_catalog','information_schema') AND
            n.nspname !~ '(^(pg|)_|_audit$)'
        )
    WHERE
        a.attnum > 0 AND
        NOT a.attisdropped
    ORDER BY c.relname, a.attnum
)
SELECT 'CREATE TABLE ' || "schema" || '.' || "table" || E' (\n    ' ||
string_agg(quote_ident("column_name" || '_old') || ' ' || column_type, E',\n    ') ||
E',\n    ' ||
string_agg(quote_ident("column_name" || '_new') || ' ' || column_type, E',\n    ') ||
$q$,
    stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
    "current_user" TEXT NOT NULL,
    "session_user" TEXT NOT NULL,
    operation TEXT NOT NULL
);

CREATE OR REPLACE FUNCTION $q$ || "schema" || '.' || "table" || $q$()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
IF (TG_OP = 'DELETE') THEN
    INSERT INTO $q$ || "schema" || '.' || "table" || E'(\n        ' ||
        string_agg(quote_ident("column_name" || '_old') , E',\n        ') || E',\n        ' ||
        string_agg(quote_ident("column_name" || '_new') , E',\n        ') || $q$,
        "current_user",
        "session_user",
        operation
    )
    VALUES(OLD.*, (NULL::$q$ || "schema" || '.' || "table" || $q$).*, current_user, session_user, TG_OP);
    RETURN OLD;
ELSIF (TG_OP = 'INSERT') THEN
    INSERT INTO $q$ || "schema" || '.' || "table" || E'(\n        ' ||
        string_agg(quote_ident("column_name" || '_old') , E',\n        ') || E',\n        ' ||
        string_agg(quote_ident("column_name" || '_new') , E',\n        ') || $q$,
        "current_user",
        "session_user",
        operation
    )
    VALUES ((NULL::$q$ || "schema" || '.' || "table" || $q$).*, NEW.*, current_user, session_user, TG_OP);
    RETURN NEW;
ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO $q$ || "schema" || '.' || "table" || E'(\n        ' ||
        string_agg(quote_ident("column_name" || '_old') , E',\n        ') || E',\n        ' ||
        string_agg(quote_ident("column_name" || '_new') , E',\n        ') || $q$,
        "current_user",
        "session_user",
        operation
    )
    VALUES (OLD.*, NEW.*, current_user, session_user, TG_OP);
    RETURN NEW;
END IF;
END;
$$;

CREATE TRIGGER $q$ || quote_ident("schema" || '_' || "table") || $q$
AFTER INSERT OR UPDATE OR DELETE ON $q$ || regexp_replace("schema" || '.' || "table", '_audit$', '') || $q$
    FOR EACH ROW EXECUTE PROCEDURE $q$ || "schema" || '.' || "table" || $q$();
$q$ AS "triggers, baby"
FROM t
group BY "schema", "table";

/* XXX Need to be able to handle adding columns.  Prolly a catalog
 * lookup.  Does 9.3 have DDL triggers we can use? */
