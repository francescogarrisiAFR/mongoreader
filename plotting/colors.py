"""This module contains functions that assign colors to data"""

import matplotlib as mpl
from numpy import linspace, random
from copy import deepcopy
from matplotlib.pyplot import get_cmap

DEFAULT_NONE_COLOR = (0.8,0.8,0.8,1)

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


def floatRanges(data:list):

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

    if not isinstance(data, list):
        raise TypeError('"data" must be a list of float/int/None.')

    if rangeMin is None or rangeMax is None:
        autoRangeMin, autoRangeMax = floatRanges(data)
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


def colorPalette(colormap, colorsNum):
    """Returns a colorsNum-long list of colors.
    
    "colormap" is a matplotlib color map. See matplotlib.pyplot.get_cmap()
    """

    values = linspace(0, 1, colorsNum)
    return [colormap(val) for val in values]


def colorDictFromStrings(colormap, strings:list):
    """Returns a color dictionary suitable for the passed list of strings.
    
    "colormap" is a matplotlib color map. See matplotlib.pyplot.get_cmap()"""

    if not isinstance(strings, list):
        raise TypeError('"strings" must be a list of strings/None.')

    stringsSet = list(set(strings))
    if None in stringsSet: stringsSet.remove(None)

    palette = colorPalette(colormap, len(stringsSet))

    colorDict = {s: c for s, c in zip(stringsSet, palette)}
    return colorDict

def stringsColors(strings:list, colormapName:str = None, NoneColor = None):
    """Returns a list of colors given a list of strings/None at the input"""

    # Automatically determining the color palette
    if colormapName is None:
        colormap = get_cmap()
    else:
        colormap = get_cmap(colormapName)

    
    colorDict = colorDictFromStrings(colormap, strings)

    colorList = [colorDict[s] if s in colorDict else NoneColor
                    for s in strings]
    return colorList
    

def randomColor():
    """Returns a random color as a 4-tuple; the first three elements are 0 to 1
    random float numbers. The last element is 1."""

    return (random.random(), random.random(), random.random(), 1)



def rawColorbar(rangeMin, rangeMax, colormapName:str = None):

    if colormapName is None:
        colormap = get_cmap()
    else:
        colormap = get_cmap(colormapName)

    norm = mpl.colors.Normalize(vmin=rangeMin, vmax=rangeMax)

    mappable = mpl.cm.ScalarMappable(norm=norm, cmap=colormap)

    return colormap, norm, mappable

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

    