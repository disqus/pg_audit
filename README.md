Audit trails for PostgreSQL tables.  Run this script in your
PostgreSQL database and use the output to create them.

To use on a database db as user u:

    psql -At -o actually_create_audit.sql -f create_audit.sql -U u db
    psql -1f actually_create_audit.sql -U u db

If you value your sanity, don't do stuff like this:

    psql -At -f create_audit.sql -U u db | psql -1 -U u db
