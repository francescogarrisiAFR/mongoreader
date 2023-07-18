"""This module contains functions that assign colors to data"""

import matplotlib as mpl
import matplotlib.patches as ptch

from numpy import linspace, random, pi
from copy import deepcopy
from matplotlib.pyplot import get_cmap

from mongomanager import log

DEFAULT_NONE_COLOR = (0.8,0.8,0.8,1)


class color:

    def __init__(self, *args):

        if len(args) == 1:
            
            if isinstance(args[0], str):
                pass

            elif isinstance(args[0], float) or isinstance(args[0], int):
                c = args[0]

                if c > 1 or c < 0:
                    raise ValueError('Color values (r, g, b, a) must be between 0 and 1.')

                self.r = c
                self.g = c
                self.b = c
                self.a = 1

            else:
                raise TypeError('Color values must be integers/float or a string')

        elif len(args) == 3:

            if any([arg < 0 or arg > 1 for arg in args]):
                raise ValueError('Color values (r, g, b, a) must be between 0 and 1.')

            self.r = args[0]
            self.g = args[1]
            self.b = args[2]
            self.a = 1

        elif len(args) == 4:

            if any([arg < 0 or arg > 1 for arg in args]):
                raise ValueError('Color values (r, g, b, a) must be between 0 and 1.')

            self.r = args[0]
            self.g = args[1]
            self.b = args[2]
            self.a = args[3]


        else:
            raise TypeError(f'color() takes 1, 3 or 4 positional arguments but {len(args)} were given')


    def __repr__(self) -> str:
        return f'color{str(self.tuple())}'

    def tuple(self):
        return (self.r, self.g, self.b, self.a)
        

def boolColor(data:bool, TrueColor = 'Red', FalseColor = 'Blue', NoneColor = None):

    if NoneColor is None:
        NoneColor = DEFAULT_NONE_COLOR

    if data is not None:
        if not isinstance(data, bool):
            raise TypeError('"data" must be a boolean or None.')

    if data is None:
        return NoneColor
    else:
        if data is True:
            return TrueColor
        else:
            return FalseColor


# Colors to float

def floatColor(data:float, colormap, rangeMin:float, rangeMax:float,
    NoneColor = None,
    clipLowColor = None,
    clipHighColor = None):
    """Data can also be an integer.
    
    "colormap" is a matplotlib color map. See matplotlib.pyplot.get_cmap()

    "clipLowColor" and "clipHighColor" are the colors returned in case data
    exceeds the range specified with "rangeMin" and "rangeMax".
    """

    if not isinstance(rangeMin, float):
        raise TypeError('"rangeMin" must be a float.')

    if not isinstance(rangeMax, float):
        raise TypeError('"rangeMax" must be a float.')

    if NoneColor is None:
        NoneColor = DEFAULT_NONE_COLOR

    if data is None:
        return NoneColor

    elif isinstance(data, int):
        data = float(data)
    
    elif not isinstance(data, float):
        raise TypeError('"data" must be a float, None, or an integer.')

    if rangeMax == rangeMin:
        return colormap(data/rangeMax)
    
    x = (data-rangeMin)/(rangeMax-rangeMin)

    # Clipped data
    if clipLowColor is not None:
        if x < 0: return clipLowColor
    if clipHighColor is not None:
        if x > 1: return clipHighColor

    return colormap(x)


def autoFloatRanges(data:list):
    """Returns a rangeMin, rangeMax tuple given a list of float/int/None."""

    def clData(data):
        cleanData = deepcopy(data)
        while None in cleanData:
            cleanData.remove(None)
        return cleanData

    cleanData = clData(data)

    if cleanData == []:
        rangeMin = 0.0
        rangeMax = 1.0
    else:
        rangeMin = float(min(cleanData))
        rangeMax = float(max(cleanData))
    
    return rangeMin, rangeMax

def floatsColors(data:list, colormapName:str = None,
    rangeMin:float = None,
    rangeMax:float = None,
    NoneColor = None,
    clipLowColor = None,
    clipHighColor = None):
    """Assigns colors to a list of float/int/None.
    
    Returns a list of colors followed by the min and max float values (range) used for the conversion."""

    if not isinstance(data, list):
        raise TypeError('"data" must be a list of float/int/None.')

    if rangeMin is None or rangeMax is None:
        autoRangeMin, autoRangeMax = autoFloatRanges(data)
        if rangeMin is None:
            rangeMin = autoRangeMin
        if rangeMax is None:
            rangeMax = autoRangeMax

    if colormapName is None:
        colormap = get_cmap()
    else:
        colormap = get_cmap(colormapName)

    return [floatColor(d, colormap, rangeMin, rangeMax,
                        NoneColor, clipLowColor, clipHighColor)
                                for d in data], rangeMin, rangeMax

# Colors to strings

def stringColor(string:str, colorDict:dict, NoneColor = None):
    """Returns a color associated to "string" as specified by the dictionary
    "colorDict".

    If string is None or if string is not found in the dict's keys, "NoneColor"
    is returned.
    """

    if not isinstance(string, str):
        raise TypeError('"string" must be a string.')
    
    if not isinstance(colorDict, dict):
        raise TypeError('"colorDict" must be a dictionary.')

    if NoneColor is None:
        NoneColor = DEFAULT_NONE_COLOR

    if string is None:
        return NoneColor
    
    try:
        return colorDict[string]
    except KeyError:
        return NoneColor


def colorPalette(colormap, colorsNum, shiftIndex = 0):
    """Returns a colorsNum-long list of colors.
    
    "colormap" is a matplotlib color map. See matplotlib.pyplot.get_cmap()
    shiftIndex (default 0): if != 0, the first "shiftIndex" colors are shipped
    in the generation.
    """

    step = 0.45*pi
    values = linspace(0, 1, colorsNum)
    values = [((n+shiftIndex)/step)%1 for n in range(colorsNum)]

    return [colormap(val) for val in values]

def stringsSetWithoutNone(strings):
    """Given a list of strings/None, it returns a set of these strings (as a
    list of strings) with None removed."""
                
    stringsSet = list(set(strings))
    if None in stringsSet: stringsSet.remove(None)
    return stringsSet


def colorDictFromStrings(colormap, strings:list, *,
                         shiftIndex = 0):
    """Returns a color dictionary suitable for the passed list of strings.
    
    "colormap" is a matplotlib color map. See matplotlib.pyplot.get_cmap().
    
    shiftIndex is passed as-is to colorPalette(). See documentation of that
    function."""

    if not isinstance(strings, list):
        raise TypeError('"strings" must be a list of strings/None.')

    stringsSet = stringsSetWithoutNone(strings)

    palette = colorPalette(colormap, len(stringsSet), shiftIndex = shiftIndex)

    colorDict = {s: c for s, c in zip(stringsSet, palette)}
    return colorDict

def stringsColors(strings:list, colormapName:str = None, NoneColor = None,
        colorDict:dict = None, *,
        expandColorDict:bool = True
        ):
    """Returns a list of colors given a list of strings/None at the input.
    
    If "expandColorDict" is True and colorDict is not None, strings not found
    in the keys of colorDict will be assigned a new color, and colorDict will
    be updated."""

    # Automatically determining the color palette
    if colormapName is None:
        colormap = get_cmap()
    else:
        colormap = get_cmap(colormapName)

    if colorDict is None:
        colorDict = colorDictFromStrings(colormap, strings)
    
    notIncludedStrings = [s for s in strings
                          if s not in colorDict]

    if notIncludedStrings != []:
        if expandColorDict: # I define colors for strings not included
            
            stringsSet = stringsSetWithoutNone(strings)
            notIncludedStringsSet = stringsSetWithoutNone(notIncludedStrings)
            shiftIndex = len(stringsSet) - len(notIncludedStringsSet)
            
            colorDictExpansion = colorDictFromStrings(colormap,
                                    notIncludedStrings,
                                    shiftIndex = shiftIndex)

            colorDict = {**colorDict, **colorDictExpansion}

    colorList = [colorDict[s] if s in colorDict else NoneColor
                    for s in strings]
    
    return colorList, colorDict
    

def randomColor(opacity = 1):
    """Returns a random color as a 4-tuple; the first three elements are 0 to 1
    random float numbers. The last element (opacity) is set to "opacity" (def. 
    1)."""

    return (random.random(), random.random(), random.random(), opacity)


def addColorBar(fig, ax, label, rangeMin, rangeMax, colormapName:str = None):
    
    if colormapName is None:
        colormap = get_cmap()
    else:
        colormap = get_cmap(colormapName)
    
    norm = mpl.colors.Normalize(vmin=rangeMin, vmax=rangeMax)
    mappable = mpl.cm.ScalarMappable(norm=norm, cmap=colormap)
    cbaxes = fig.add_axes([0.9, 0.1, 0.04, 0.8])
    # cbaxes.tick_params(labelsize=20)
    # cbaxes.set_fontsize(20)
    cb = fig.colorbar(mappable, norm = norm, cax = cbaxes, orientation='vertical')
    if label is not None:
        cb.set_label(label, fontsize = 20, labelpad = 8)
    cb.ax.tick_params(labelsize=20)
    return fig, ax

    
def addLegend(fig, legendDict = None, legendFontSize = None):

    # Arguments checks

    if legendDict is not None:
        if not isinstance(legendDict, dict):
            raise TypeError('"legendDict" must be a dictionary.')

    if legendDict is None: legendDict = {}

    if legendFontSize is None: legendFontSize = 15

    log.debug(f'[addLegend] legendDict: {legendDict}')

    for text, color in legendDict.items():
        if color is None:
            raise ValueError('"legendDict" cannot contain None as color.')


    # -------------------------------------
    # Settings and parameters

    # position of legend axes

    axes_x = -0.1
    axes_y = 0.1

    # Size of legend axes
    w = 0.2
    h = 0.8
    ar = h/w

    # squares and text

    _rows = 15 # Numbers of rows in the legend. Determines the spacing
    squareSize = 0.1
    square_x = 0.1
    text_x = 0.3
    

    # -------------------------------------
    # Functions

    def addSquare(ax, x, y, faceColor, squareSize = squareSize):

        realw = squareSize
        realh = squareSize/ar

        rect = ptch.Rectangle((x,y), realw, realh,
                edgecolor = 'black',
                facecolor = faceColor,
                # lw = linewidth,
                # fill=fill
                )

        # rect = ptch.Rectangle()

        ax.add_patch(rect)

    def addText(ax, x, y, text:str):
        ax.text(x, y, text, fontsize = legendFontSize)


    # -------------------------------------
    # Bulk of the function
    
    # Adding legend axes
    ax = fig.add_axes([axes_x, axes_y, w, h])
    ax.axis('off') # Disabling frame

    
    _ys = linspace(0.9, 0.1, _rows)

    for ind, (text, color) in enumerate(legendDict.items()):

        log.debug(f'[addLegend] Adding color "{color}" with label "{text}"')

        y = _ys[ind]
        xsq = square_x
        xtxt = text_x

        addSquare(ax, xsq, y, color, squareSize)
        addText(ax, xtxt, y, text)


    return fig