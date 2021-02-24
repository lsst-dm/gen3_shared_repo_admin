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

A simple example: the ``tract`` table
-------------------------------------

The ``tract`` table is a static table (it's created when the data repository is created).
We assume all static tables are created (and thus owned) by the ``repo_admin`` role, which is initially the only (non-superuser) role that can do anything with that table.
The access controls we'll define for ``tract`` are the same in all repositories, and they don't involve any row-level security.

When ``butler`` code creates the ``tract`` table, it will look in the ``post_create_hooks.static`` section for matching hooks.
In this case, that just involves the ``base.yaml`` file here.
It will find:

- a generic ``before`` hook that is run after the table is created, but before any other hooks (shared with all other static tables);
- a ``by_name.tract`` hook specific to this table that is run next;
- a generic ``after`` hook that is run after the table-specific hook, though in this case the ``after`` hook is any empty list, so it does nothing.

The ``default`` hook in the ``static`` section is ignored, because a match in ``by_name`` was found.
The ``butler`` Python code will then concatenate these lists of strings (with semicolons) and process the ``{}`` placeholders they contain.
The ``{table}`` placeholder is replaced with ``"tract"`` (the name of the table), and the ``{snippets.shared_insert}`` placeholder is replaced by the text in the ``post_create_hooks.snippets.shared_insert`` configuration entry.
Putting it all together, the post-creation hook is:

.. code:: sql

    GRANT SELECT, REFERENCES ON tract TO PUBLIC;
    GRANT INSERT, UPDATE, DELETE ON tract TO shared;

This lets all users (via the PostgreSQL special ``PUBLIC`` role) read rows from the table and create new tables that reference this table in foreign keys.
It also lets the ``shared`` role insert, update, and delete on this table.
That ``shared`` role is defined over in ``base.sql``:

.. code:: sql

    CREATE ROLE shared INHERIT;

This group-like role doesn't have any direct (``LOGIN``) members, but we have another group-like role, ``shared_members``, that could:

.. code:: sql

    CREATE ROLE shared_members NOINHERIT;

We've granted permissions to ``shared`` instead of ``shared_members`` so that members have to explicitly "assume" the shared role (this is what the ``NOINHERIT`` enforces), instead of being able to accidentally (for example) create new tracts without meaning to.
They do that via ``SET ROLE shared``, which is something we'll have our Python code do for them when they pass something like ``role="shared"`` to a butler Python interface.

However, in this particular case, we actually then go ahead and let *any* user assume the ``shared`` role:

.. code:: sql

    GRANT shared_members TO users;

Because ``users`` is defined with ``INHERIT``, members of ``users`` (who must also be defined with ``INHERIT``) can assume ``shared`` without having to assume ``shared_members`` along the way.
But because ``shared_members`` is ``NOINHERIT``, they can't do things ``shared`` can do until they do assume ``shared``.

A complex example: ``dataset_tags_*`` tables
--------------------------------------------

Butler creates tables with names that begin with ``dataset_tags_`` on the fly, when users register dataset types identified by a set of dimensions that haven't been used together before.
We call these tables "dynamic", and they'll start out being owned by whatever user created them, but after setting up access controls on those, we'll transfer ownership back to the ``repo_admin`` role that owns all static tables.
The access controls for ``dataset_tags_*`` are complex because they involve row-level security: whether a role is allowed to manipulate a row depends on some rules based on the value of one of its columns.
Those roles and rules are slightly different in different repositories, reflecting the different kinds of data in those repositories.

When ``butler`` code creates a ``dataset_tags_*`` table, it will look in the ``post_create_hooks.dynamic`` section for matching hooks.
For a complete repository, that configuration will be formed by merging the ``base.yaml`` file with one of the per-repo YAML files; we'll focus on ``main.yaml`` here.

The matching hooks it will find include:

- a generic ``before`` hook that is run after the table is created, but before any other hooks (shared with all other dynamic tables);
- a matching ``by_prefix.dataset_tags_`` hook that is run next;
- a generic ``after`` hook that is run after the table-specific hook (this is what transfers ownership back to ``repo_admin``).

The ``default`` hook in the ``dynamic`` section is ignored, because a match in ``by_prefix`` was found.
The ``butler`` Python code will then concatenate these lists of strings (with semicolons) and process the ``{}`` placeholders they contain.

The ``by_prefix`` hook is a particularly complex placeholder.
It starts with this:

.. code:: yaml

    dataset_: ["{snippets.collection_policies(column='collection_name')}"]

This snippet can be found in ``base.yaml``, and the argument in parenthesis says to replace ``{column}`` in that snippet with the literal ``collection_name``, which is the name of the column in a ``dataset_tags_*`` table that is used for access controls.
Hooks for other tables that use the same snippet pass a different column name, reflecting the name of the column with that content in each table.
Expanding that into the snippet definition for ``collection_policies``, we get

.. code:: yaml

    dataset_:
        - "{snippits.init_row_security}"
        - "GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO PUBLIC"
        - "CREATE POLICY user_insert ON {table} FOR INSERT USING (snippets.is_user_collection(column={column}))"
        - "CREATE POLICY user_update ON {table} FOR UPDATE USING (snippets.is_user_collection(column={column}))"
        - "CREATE POLICY user_delete ON {table} FOR DELETE USING (snippets.is_user_collection(column={column}))"
        - "CREATE POLICY processing_insert ON {table} FOR INSERT TO processing USING (snippets.is_processing_collection(column={column})"
        - "CREATE POLICY processing_update ON {table} FOR UPDATE TO processing USING (snippets.is_processing_collection(column={column})"
        - "CREATE POLICY processing_delete ON {table} FOR DELETE TO processing USING (snippets.is_processing_collection(column={column})"
        - "CREATE POLICY cpp_insert ON {table} FOR INSERT TO cpp USING ({snippets.is_cpp_collection(column={column})})"
        - "CREATE POLICY cpp_update ON {table} FOR UPDATE TO cpp USING ({snippets.is_cpp_collection(column={column})})"
        - "CREATE POLICY cpp_delete ON {table} FOR DELETE TO cpp USING ({snippets.is_cpp_collection(column={column})})"
        - "CREATE POLICY shared_insert ON {table} FOR INSERT TO shared USING ({snippets.is_shared_collection(column={column})})"
        - "CREATE POLICY shared_update ON {table} FOR UPDATE TO shared USING ({snippets.is_shared_collection(column={column})})"
        - "CREATE POLICY shared_delete ON {table} FOR DELETE TO shared USING ({snippets.is_shared_collection(column={column})})"
        - "{snippets.collection_policies_repo}"  # repo-specific policies from a to-be-merged file

That in turn references many more snippets.
The first of these, ``init_row_security``, sets up row-level security for a table while preserving the usual starting table-level security (everyone can read, ``repo_admin`` can do anything) in that model:

.. code:: yaml

    init_row_security:
        - "ALTER TABLE {table} ENABLE ROW SECURITY"
        - "CREATE POLICY public_read ON {table} FOR SELECT USING (true)"
        - "CREATE POLICY admin_all ON {table} FOR ALL TO repo_admin USING (true)"

The ``CREATE POLICY`` lines each define a row-level security rule that allows inserts, updates, or deletes for one role, based on a collection name pattern.
That pattern matching is delegated to another set of snippets.
We'll look at ``is_processing_collection`` more closely:

.. code:: yaml

    is_processing_collection:
      "STARTS_WITH({column}, 'runs/') OR (SELECT regex_match({column}, '([^/]+/)runs/.+'))[1] IN SELECT name FROM instrument"

Here we finally use that column placeholder in actual SQL, and the logic says "allow this operation if the ``collection_name`` value starts with 'runs/' or '<instrument>/runs/', where '<instrument>' is a value from the ``instrument`` table.
Check out the `PostgreSQL documentation`_ for ``regex_match`` for details on the syntax.

The snippet at the very end is one that's defined in ``main.yaml`` or another per-repo configuration file that's merged in.
It defines a similar set of policies for roles that exist only in that repo.

As with the ``tract`` case, most of the roles mentioned in these policies are defined in ``base.sql``, while those referenced by the policies in ``main.yaml`` are defined in ``main.sql``.

.. _`PostgreSQL documentation`: https://www.postgresql.org/docs/12/functions-matching.html#FUNCTIONS-POSIX-REGEXP
