# This file is part of gen3_shared_repo_admin
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

# SkyMap used for all HSC SSP processing.

config.skyMap.name = "rings"
config.skyMap["rings"].numRings = 120
config.skyMap["rings"].projection = "TAN"
config.skyMap["rings"].tractOverlap = 1.0/60  # Overlap between tracts (degrees)
config.skyMap["rings"].pixelScale = 0.168
config.name = "hsc_rings_v1"