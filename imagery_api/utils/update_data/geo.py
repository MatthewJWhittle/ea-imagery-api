# -*- coding: utf-8 -*-
# ##########################################################################
#
#  COPYRIGHT:  (C) 2012-2019 John A Stevenson / @volcan01010
#                            Magnus Hagdorn / @mhagdorn
#                            Jumy Elǝrossë / @realjumy
#  WEBSITE: http://all-geo.org/volcan01010
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  http://www.gnu.org/licenses/gpl-3.0.html
#
# ############################################################################
import re

__version__ = '1.0.3'
__all__ = ['to_osgb36', 'from_osgb36']


class BNGError(Exception):
    """Exception raised by bng.py module"""
    pass


def _init_regions_and_offsets():
    # Region codes for 100 km grid squares.
    regions = [['HL', 'HM', 'HN', 'HO', 'HP', 'JL', 'JM'],
               ['HQ', 'HR', 'HS', 'HT', 'HU', 'JQ', 'JR'],
               ['HV', 'HW', 'HX', 'HY', 'HZ', 'JV', 'JW'],
               ['NA', 'NB', 'NC', 'ND', 'NE', 'OA', 'OB'],
               ['NF', 'NG', 'NH', 'NJ', 'NK', 'OF', 'OG'],
               ['NL', 'NM', 'NN', 'NO', 'NP', 'OL', 'OM'],
               ['NQ', 'NR', 'NS', 'NT', 'NU', 'OQ', 'OR'],
               ['NV', 'NW', 'NX', 'NY', 'NZ', 'OV', 'OW'],
               ['SA', 'SB', 'SC', 'SD', 'SE', 'TA', 'TB'],
               ['SF', 'SG', 'SH', 'SJ', 'SK', 'TF', 'TG'],
               ['SL', 'SM', 'SN', 'SO', 'SP', 'TL', 'TM'],
               ['SQ', 'SR', 'SS', 'ST', 'SU', 'TQ', 'TR'],
               ['SV', 'SW', 'SX', 'SY', 'SZ', 'TV', 'TW']]

    # Transpose so that index corresponds to offset
    regions = list(zip(*regions[::-1]))

    # Create mapping to access offsets from region codes
    offset_map = {}
    for i in range(len(regions)):
        for j in range(len(regions[0])):
            region = regions[i][j]
            offset_map[region] = (1e5 * i, 1e5 * j)

    return regions, offset_map


_regions, _offset_map = _init_regions_and_offsets()


def to_osgb36(gridref):
    """
    Convert British National Grid references to OSGB36 numeric coordinates.
    Grid references can be 4, 6, 8 or 10 figures.

    :param gridref: str - BNG grid reference
    :returns coords: tuple - x, y coordinates

    Examples:

    Single value
    >>> to_osgb36('NT2755072950')
    (327550, 672950)

    For multiple values, use Python's zip function and list comprehension
    >>> gridrefs = ['HU431392', 'SJ637560', 'TV374354']
    >>> x, y = zip(*[bng.to_osgb36(g) for g in gridrefs])
    >>> x
    (443100, 363700, 537400)
    >>> y
    (1139200, 356000, 35400)
    """
    # Validate input
    bad_input_message = (
        'Valid gridref inputs are 4, 6, 8 or 10-fig references as strings '
        'e.g. "NN123321", or lists/tuples/arrays of strings. '
        '[{}]'.format(gridref))

    try:
        gridref = gridref.upper()
        pattern = r'^([A-Z]{2})(\d{4}|\d{6}|\d{8}|\d{10})$'
        match = re.match(pattern, gridref)
    except (TypeError, AttributeError):
        # Non-string values will throw error
        raise BNGError(bad_input_message)

    if not match:
        raise BNGError(bad_input_message)

    # Extract data from gridref
    region, coords = match.groups()

    # Get offset from region
    try:
        x_offset, y_offset = _offset_map[region]
    except KeyError:
        raise BNGError('Invalid 100 km grid square code: {}'.format(region))

    # Get easting and northing from text and convert to coords
    half_figs = len(coords) // 2
    easting, northing = int(coords[:half_figs]), int(coords[half_figs:])
    scale_factor = 10 ** (5 - half_figs)
    x = int(easting * scale_factor + x_offset)
    y = int(northing * scale_factor + y_offset)
    
    resolution = 10 ** (6 - (half_figs))

    return x, y, resolution
