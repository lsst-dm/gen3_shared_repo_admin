-- This script is run manually before a repository is created, after base.sql,
-- and is intended only for the 'main' repository that contains all real data.

CREATE ROLE hsc_admin_members NOINHERIT;
CREATE ROLE hsc_admin INHERIT;
GRANT hsc_admin TO hsc_admin_members;

CREATE ROLE decam_admin_members NOINHERIT;
CREATE ROLE decam_admin INHERIT;
GRANT decam_admin TO decam_admin_members;

CREATE ROLE lsstdata_admin_members NOINHERIT;
CREATE ROLE lsstdata_admin INHERIT;
GRANT lsstdata_admin TO lsstdata_admin_members;
