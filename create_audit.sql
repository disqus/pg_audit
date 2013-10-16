WITH t AS (
    SELECT
        quote_ident(n.nspname) AS "schema" || '.' || quote_ident(c.relname) || '_audit' AS "audit",
        quote_ident(a.attname) AS "column_name",
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
            n.nspname !~ '^_'
        )
    WHERE
        a.attnum > 0 AND
        NOT a.attisdropped
    ORDER BY c.relname, a.attnum
)
SELECT 'CREATE TABLE ' || "audit" || E'(\n    ' ||
string_agg("column_name" || '_old ' || column_type, E',\n    ') ||
E',\n    ' ||
string_agg("column_name" || '_new ' || column_type, E',\n    ') ||
$q$,
    stamp timestamp with time zone default now(),
    operation text not null
);

CREATE OR REPLACE FUNCTION $q$ || "audit" || $q$ RETURNS TRIGGER AS
$$BEGIN
IF (TG_OP = 'DELETE') THEN
    INSERT INTO $q$ || "audit" || E'(\n        ' ||
    string_agg(quote_ident("column_name") || '_old ' || column_type, E',\n        ') || $q$,
        operation
    )
    VALUES(OLD.*, TG_OP);
ELSIF (TG_OP = 'INSERT') THEN
    INSERT INTO $q$ || "audit" || E'(\n        ' ||
    string_agg(quote_ident("column_name") || '_new ' || column_type, E',\n        ') || $q$,
        operation
    )
    VALUES (NEW.*, TG_OP);
ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO $q$ || "audit" || E'(\n        ' ||
    string_agg(quote_ident("column_name") || '_old ' || column_type, E',\n        ') || E',\n        ' ||
    string_agg(quote_ident("column_name") || '_new ' || column_type, E',\n        ') || $q$,
        operation
    )
    VALUES (NEW.*, TG_OP);
END IF;
RETURN NULL;
END;
$$;

CREATE TRIGGER $q$ || "audit" || $q$
AFTER INSERT OR UPDATE OR DELETE ON $q$ || regexp_replace("audit", '_audit$', '') || $q$
    FOR EACH ROW EXECUTE PROCEDURE $q$ || "audit" || $q$();
$q$ AS "triggers, baby"
FROM t
GROUP BY "audit";
