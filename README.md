Audit trails for PostgreSQL tables.  Run this script in your
PostgreSQL database and use the output to create them.

To use on a database db as user u:

    psql -AtX -o actually_create_audit.sql -f create_audit.sql -U u db
    psql -X -1f actually_create_audit.sql -U u db

If you value your sanity, don't do stuff like this:

    psql -AtX -f create_audit.sql -U u db | psql -X -1 -U u db
