# SkyMap configuration files

The files in this directory are
[`pex_config`](https://github.com/lsst/pex_config) configuration files for the
sky coordinate systems defined in [`skymap`](https://github.com/lsst/skymap).
Most of them are wholly or partially copied from `obs_*` packages, and we copy
rather than link to avoid future breakage due to organizations.  SkyMap
definitions must be considered immutable after they have been used by any major
processing effort, to avoid breaking access to that data, so we do not need to
worry about these getting out of sync with the `obs_*` packages.
