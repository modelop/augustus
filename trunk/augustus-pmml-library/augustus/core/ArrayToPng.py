#!/usr/bin/env python

# Copyright (C) 2006-2013  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
#
# This file is part of Augustus.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This module defines the ArrayToPng class."""

import struct
import zlib
import base64
import StringIO

from augustus.core.NumpyInterface import NP

class ArrayToPng(object):
    """Helper object for converting Numpy arrays into PNG images.

    We do not use U{PIL<http://www.pythonware.com/products/pil/>}
    because it is difficult to embed in some systems and we want to
    avoid the additional dependency.

    ArrayToPng is typically used to embed raster images in SVG.  Here
    is an example use::

        arrayToPng = ArrayToPng()
        arrayToPng.putdata(xbins, ybins, reddata, greendata, bluedata, alphadata)
        svg.image(**{defs.XLINK_HREF: "data:image/png;base64," + arrayToPng.b64encode(),
                  "x": repr(X1), "y": repr(Y2), "width": repr(X2 - X1), "height": repr(Y1 - Y2)})
    """

    def __init__(self, file=None):
        """Create an ArrayToPng with an empty internal buffer."""

        if file is None:
            self.file = StringIO.StringIO()
        else:
            self.file = file

    def _writeChunk(self, tag, data):
        """Used by putdata."""

        self.file.write(struct.pack("!I", len(data)))
        self.file.write(tag)
        self.file.write(data)
        cyclicRedundancyCheck = zlib.crc32(tag)
        cyclicRedundancyCheck = zlib.crc32(data, cyclicRedundancyCheck)
        cyclicRedundancyCheck &= 0xffffffff
        self.file.write(struct.pack("!I", cyclicRedundancyCheck))
        
    def putdata(self, width, height, red, green, blue, alpha, flipy=True, onePixelBeyondBorder=False):
        """Fill the internal buffer with a PNG-encoded version of arrays red, green, and blue.

        @type width: int
        @param width: The width of the image.
        @type height: int
        @param height: The height of the image.
        @type red: 1d Numpy array of length C{width*height} and type uint8
        @param red: Red channel of the image as a 1d array.
        @type green: 1d Numpy array of length C{width*height} and type uint8
        @param green: Green channel of the image as a 1d array.
        @type blue: 1d Numpy array of length C{width*height} and type uint8
        @param blue: Blue channel of the image as a 1d array.
        @type alpha: 1d Numpy array of length C{width*height} and type uint8
        @param alpha: Alpha channel of the image as a 1d array.
        @type flipy: bool
        @param flipy: If True, flip the y coordinate of the image so that y increases upward in a plot but downward in the PNG file.
        @type onePixelBeyondBorder: bool
        @param onePixelBeyondBorder: If True, expand the image by one pixel along all borders, filling in the pixels with their nearest neighbors.  Some SVG renderers blur the edge of an embedded PNG into the background, which is misleading when the PNG represents a heatmap in a plot.
        """

        # red, green, blue, alpha are assumed to be flat, uint8 Numpy arrays of the same length
        interleaved = NP("empty", 4 * width * height, dtype=NP.uint8)
        interleaved[0::4] = red
        interleaved[1::4] = green
        interleaved[2::4] = blue
        interleaved[3::4] = alpha
        interleaved = NP("reshape", interleaved, (height, 4 * width))

        if flipy:
            interleaved = interleaved[-1::-1,:]

        if onePixelBeyondBorder:
            width += 2
            height += 2

            scanlines = NP("empty", (height, 4 * width + 1), dtype=NP.uint8)
            scanlines[:,0] = 0  # first byte of each scanline is zero

            scanlines[1:-1,5:-4] = interleaved
            scanlines[1:-1,1:5] = scanlines[1:-1,5:9]
            scanlines[1:-1,-4:] = scanlines[1:-1,-8:-4]

            scanlines[0,:] = scanlines[1,:]
            scanlines[-1,:] = scanlines[-2,:]

            scanlines = NP("reshape", scanlines, height * (4 * width + 1))
            
        else:
            scanlines = NP("empty", (height, 4 * width + 1), dtype=NP.uint8)
            scanlines[:,0] = 0  # first byte of each scanline is zero
            scanlines[:,1:] = interleaved
            scanlines = NP("reshape", scanlines, height * (4 * width + 1))

        self.file.write("\211PNG\r\n\032\n")
        self._writeChunk("IHDR", struct.pack("!2I5B", width, height, 8, 6, 0, 0, 0))
        self._writeChunk("IDAT", zlib.compress(str(scanlines.data)))
        self._writeChunk("IEND", "")

    def close(self):
        """Close the internal buffer.

        Must not be called before C{putdata}.
        """

        self.file.close()

    def b64encode(self):
        """Base64-encode the serialized image.

        Must not be called before C{putdata}.

        @rtype: string
        @return: A base64-encoded version of the currently loaded image.
        """

        return base64.b64encode(self.file.getvalue())
