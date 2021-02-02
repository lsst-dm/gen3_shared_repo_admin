#############################
Database-side access controls
#############################

This directory contains SQL scripts to be run by administrators prior to repo creation and YAML butler configuration files to use during repository creation in order to set up database access controls.
The ``base.*`` files are common to all of the NCSA shared data repositories, and there are (or will be) individual files for each data repository as well, accounting for the differences in instruments and conventions between those repositories.
In particular:
- ``base.sql`` defines roles common to all data repositories (but not membership in those roles; we assume that is done some other way by DB admins);
- ``main.sql``, ``DC2.sql``: additional roles specific to those repos;
- ``base.yaml``: butler configuration with access control roles, as post-table-creation hooks;
- ``main.yaml``, ``DC2.yaml``: butler configuration files to merge with ``base.yaml`` before using that configuration to create a data repository.

These access controls assume friendly users who access the database through the butler client (not direct SQL access), as they very much aim to prevent accidental misuse, not intentional misuse.

They also assume logic in ``daf_butler`` for executing configured SQL hooks after table creation that *does not yet exist*.
The YAML configuration files here are the only examples of and documentation for that system; we're trying it out on this use case before implementing it.

Finally, the scripts and configuration here assume that the registry is configured to use the ``NameKeyCollectionManager`` instead of the default ``SynthIntKeyCollectionManager``; this makes the row-level security rules for datasets much simpler, because there's no indirection necessary to get from the table we want to protect to the collection name that sets who can access it.
