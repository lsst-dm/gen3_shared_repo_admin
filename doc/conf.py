"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.gen3.shared.repo.admin


_g = globals()
_g.update(build_package_configs(
    project_name='gen3_shared_repo_admin',
    version=lsst.gen3.shared.repo.admin.version.__version__))
