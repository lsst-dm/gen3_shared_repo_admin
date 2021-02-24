-- This script is run manually before a repository is created, after base.sql,
-- and is intended only for the 'main' repository that contains all real data.

CREATE ROLE dc2_admin_members NOINHERIT;
CREATE ROLE dc2_admin INHERIT;
GRANT dc2_admin TO dc2_admin_members;
