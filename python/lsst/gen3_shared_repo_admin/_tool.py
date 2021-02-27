# This file is part of gen3_shared_repo_admin.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("RepoAdminTool",)

import logging

from tqdm import tqdm

from lsst.daf.butler import Butler, ButlerURI, Config, DimensionConfig

from ._dataclasses import RepoDefinition, SiteDefinition
from .definitions import REPOS, SITES


class StepNotReadyError(RuntimeError):
    pass


class RepoAdminTool:

    def __init__(self, repo: RepoDefinition, site: SiteDefinition, dry_run: bool = False):
        self.repo = repo
        self.site = site
        self._butler = None
        self.dry_run = dry_run
        if dry_run:
            self.log = logging.getLogger(f"butler-admin (testing)")
        else:
            self.log = logging.getLogger(f"butler-admin")

    @classmethod
    def from_strings(cls, repo: str, site: str, date: str, dry_run: bool = False) -> RepoAdminTool:
        return cls(REPOS[repo][date], SITES[site], dry_run=dry_run)

    @property
    def root(self) -> str:
        return self.site.repo_uri_template.format(repo=self.repo)

    def make_butler_config(self) -> Config:
        config = Config()
        config[".registry.db"] = self.site.db_uri_template.format(repo=self.repo)
        config[".registry.namespace"] = self.site.db_namespace_template.format(repo=self.repo)
        for template in self.repo.butler_config_templates:
            uri = ButlerURI(template.format(repo=self.repo, site=self.site))
            if uri.exists():
                config.update(Config(uri))
        return config

    def make_dimension_config(self) -> DimensionConfig:
        config = DimensionConfig()
        for template in self.repo.dimension_config_templates:
            uri = ButlerURI(template.format(repo=self.repo, site=self.site))
            if uri.exists():
                config.update(Config(uri))
        return config

    def create(self) -> None:
        self.log.info("Creating empty repository at %s.", self.root)
        if not self.dry_run:
            Butler.makeRepo(self.root, config=self.make_butler_config(),
                            dimensionConfig=self.make_dimension_config())

    @property
    def butler(self) -> Butler:
        if self._butler is None:
            try:
                self._butler = Butler(self.root, writeable=not self.dry_run)
            except FileNotFoundError:
                raise StepNotReadyError("Repo has not yet been created.")
        return self._butler

    def register_skymaps(self, resume: bool = False):
        self.log.info("Registering SkyMaps in %s.", self.root)
        from lsst.pipe.tasks.script.registerSkymap import MakeSkyMapConfig
        todo = {}
        for uri in tqdm(self.repo.skymaps, desc="Loading SkyMap configuration"):
            config = MakeSkyMapConfig()
            config.loadFromStream(ButlerURI(uri).read().decode())
            todo[config.name] = config
        if resume:
            existing = {r.name for r in tqdm(self.butler.queryDimensionRecords("skymap"),
                                             desc="Finding already-registered SkyMaps")}
            for name in existing:
                del todo[name]
                self.log.info("SkyMap '%s' is already registered; skipping.", name)
        for config in tqdm(todo.values(), desc="Constructing and registering SkyMaps"):
            self.log.info("Constructing SkyMap '%s' from configuration.", config.name)
            skymap = config.skyMap.apply()
            skymap.logSkyMapInfo(self.log)
            self.log.info("Registering SkyMap '%s' in database.", config.name)
            if not self.dry_run:
                skymap.register(config.name, self.butler)
