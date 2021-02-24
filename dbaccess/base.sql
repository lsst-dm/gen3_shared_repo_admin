-- This script is run manually before a repository is created.
--
-- It sets up most group-like roles, but does not populate them with actual
-- users (that's not something done by these scripts at all).
--
-- One of the per-repo scripts (main.sql, DC2.sql) should also be run after
-- this script and before repo creation.

-- We define most group roles in pairs: one to which users are added directly
-- (with NOINHERIT), and another that actually has group-specific permissions
-- (with INHERIT, though this _usually_ doesn't matter).  Users are expected to
-- "assume" the latter role with `SET ROLE` (or, rather, Python code that does
-- that) only when performing work that requires that group's permissions.

-- repo_admin members are superusers within the schemas that correspond to Gen3
-- repos.
--
-- This role is used to create the data repository, so it owns all static
-- tables automatically.  Dynamic table ownership will be transferred to this
-- role by our post-creation hooks (see base.yaml).
CREATE ROLE repo_admin_members NOINHERIT;
CREATE ROLE repo_admin INHERIT;
GRANT repo_admin TO repo_admin_members;

-- processing members manage official processing runs.
CREATE ROLE processing_members NOINHERIT;
CREATE ROLE processing INHERIT;
GRANT processing TO processing_members;

-- cpp members manage calibration products for other users.
CREATE ROLE cpp_members NOINHERIT;
CREATE ROLE cpp INHERIT;
GRANT cpp TO cpp_members;

-- The shared role is for things that anyone can manage (usually just create),
-- but only when explicitly opting in.
CREATE ROLE shared_members NOINHERIT;
CREATE ROLE shared INHERIT;
GRANT shared TO shared_members;

-- All users are members of the users group.  Permissions are directly granted
-- to this group, including permission to assume certain other roles.
CREATE ROLE users INHERIT;

-- TODO: users should be able to create tables (including temporary tables).
-- And probably other things they can currently do that I've forgotten about.
-- I assume the setup for the current Gen3 repos will tell us what I've missed.

-- All users can assume the `shared` role, but must do so explicitly; they will
-- do this rarely, so assuming this role is in the spirit of a "yes, I'm sure
-- I want to" check.
GRANT shared_members TO users;
