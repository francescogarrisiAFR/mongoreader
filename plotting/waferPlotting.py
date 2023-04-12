"""This module contains functions and classes useful to plot data on wafer
shapes"""

import matplotlib.pyplot as plt
import json
import datetime as dt

# from . import patches as p
# from . import colors as c

from mongoreader.plotting import patches as p
from mongoreader.plotting import colors as c

from mongomanager import (
        log,
        waferBlueprint,
        importWaferBlueprint,
        isID,
        opened
)

from pathlib import Path
from numpy import random, linspace



class _waferPlotter:
    """This class should not be instantiated directly, but only through its sub-classes"""


    def __init__(self, connection, notch = None, *,
        waferBP: waferBlueprint):

        if not isinstance(waferBP, waferBlueprint):
            raise TypeError('"waferBP" must be a mongomanager.waferBlueprint object.')
        
        with opened(connection):
            self.waferBP = waferBP
            self.chipP1P2s = waferBP.retrieveChipP1P2s(connection)
            self.allowedGroups = waferBP.getWaferChipGroupNames()
            self.allChipLabels = waferBP.getWaferChipSerials()

        # Should be retrieved from waferBP's "geometry" field.
        self.D = 101.6 # Wafer plot diameter in mm
        if notch is None:
            self.notch = 2.7

    def _chipGroupLabels(self, groups:list):

        if groups is None:
            return self.allChipLabels

        allowedGroupString = ', '.join([f'"{g}"' for g in self.allowedGroups])

        if not isinstance(groups, list):
                raise TypeError(f'"groups" must be a list of strings among {allowedGroupString}.')

        for el in groups:
            if not el in self.allowedGroups:
                raise ValueError(f'"groups" must be a list of strings among {allowedGroupString}.')

        labels = []
        for group in groups:
            labels += self.waferBP.getWaferChipSerials(group)
        return labels
        
    
    def _chipP1P2(self, chipLabel:str):
        """Returnes the p1-p2 pairs of points that identify chip rectangles on wafer."""

        if not chipLabel in self.allChipLabels:
            raise ValueError(f'"chipLabel" ("{chipLabel}") is not valid.')
        
        p12 = self.chipP1P2s[chipLabel] # um

        p1 = list(map(lambda x: float(x)/1000, p12["p1"])) # um to mm
        p2 = list(map(lambda x: float(x)/1000, p12["p2"])) # um to mm

        log.debug(f'[_chipP1P2] "{chipLabel}" p1: {p1} - p2: {p2}')

        return p1, p2

    def _chipPatch(self, chipLabel:str, color = None):
        """Returnes the (possibily colored) rectangle patch for a labeled chip."""

        p1, p2 = self._chipP1P2(chipLabel)
        
        return p.rectPatch(p1, p2, color)

    
    def _chipSubPatches(self, chipLabel:str, colors:list = None):
        """Returns a list of rectangles (possibily colored) that fill the space
        of a chip on the wafer.
        
        The number of sub-sections is determined from the kind of chip.
        DR4/FR4 -> 4
        DR8/FR8 -> 4
        
        If "colors" is passed, it must be a list as long as the number of
        subsections. "colors" may contain colors or None.

        """
        p1, p2 = self._chipP1P2(chipLabel)
        
        subSections = len(colors)

        log.debug(f'[_chipSubPatches] {chipLabel}: {p1} - {p2}')
        log.debug(f'[_chipSubPatches] subSections: {subSections}')
        log.debug(f'[_chipSubPatches] colors: {colors}')
        
        rects = p.rectSubPatches(p1, p2, subSections, colors)
        return rects


    def _addChipLabelText(self, fig, ax, chipLabel:str, direction:str = None):

        if direction is not None:
            if not isinstance(direction, str):
                raise TypeError('"direction" must be None or a string among "North", "East", "South", "West".')

        p1, p2 = self._chipP1P2(chipLabel)
        x0, x1, y0, y1, width, height = p._unpackP12(p1, p2)

        textx = x0+width/2
        texty = y0+height/2
        ha = 'center'
        va = 'center'

        if direction is not None:

            if direction == 'North':
                texty += 0.8*height

            elif direction == 'South':
                texty -= 0.8*height

            elif direction == 'East':
                textx = x1 + 0.2*width
                ha = 'left'

            elif direction == 'West':
                textx = x0 - 0.2*width
                ha = 'right'
            
            else:
                raise ValueError('"direction" is not among "North", "East", "South", "West".')

        ax.text(textx, texty, chipLabel, fontsize = 10,
            ha = ha,
            va = va)


    # Plot methods

    def _plot(self, patches:list,
            rangeMin, rangeMax,
            colormapName,
            *,
            chipGroups:list = None,
            printBar:bool = True,
            barLabel:str = None,
            legendDict:dict = None,
            legendFontSize = None,
            title:str = None,
            waferName:str = None,
            printDate:bool = True,
            printLabels:bool = True,
            labelsDirection:str = None,
            dpi = None
            ):
        """This is the main function used to realize wafer-plots."""

        if dpi is None: dpi = 200

        plt.figure(dpi = dpi, figsize = (10, 10))
        fig, ax = plt.subplots() # note we must use plt.subplots, not plt.subplot
        fig.set_size_inches(10, 10, forward=True)
        
        for ptc in patches:
            ax.add_patch(ptc)
        
        # Chip labels:
        if printLabels:
            for label in self._chipGroupLabels(chipGroups):
                self._addChipLabelText(fig, ax, label, direction = labelsDirection)

        # Wafer name
        if waferName is not None:
            ax.text(0, -1.05*self.D/2, waferName,
                fontsize = 20,
                ha = 'center')

        # AFR confidential
        ax.text(-self.D/2, -1.03*self.D/2, 'AFR confidential',
            fontsize = 14)

        # Date
        if printDate:
            ax.text(-self.D/2, -0.95*self.D/2, dt.date.today().strftime('%d/%m/%Y'),
                fontsize = 12)

        ax.set_xlim(-((self.D/2)+2),((self.D/2)+2))
        ax.set_ylim(-((self.D/2)+2),((self.D/2)+2))

        plt.axis('off')
        if title is not None:
            ax.text(0, 1.05*self.D/2, title, fontsize = 24, ha = 'center')
        plt.autoscale(enable=True, axis='both')

        if printBar:
            c.addColorBar(fig, ax, barLabel, rangeMin, rangeMax, colormapName)

        if legendDict is not None:
            c.addLegend(fig, legendDict, legendFontSize)

        plt.show()

        return fig, ax


    def _allChipSubpatches(self, dataDict:dict,
        dataType:str,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        colormapName:str = None,
        colorDict:dict = None,
        NoneColor = None,
        clippingLowColor = None,
        clippingHighColor = None,
        chipGroups:list = None,
        ):
        """Returns all the subpatches to be plotted by _plot."""

        if NoneColor is None: NoneColor = c.DEFAULT_NONE_COLOR
        
        rangeMin, rangeMax = None, None

        if dataType == 'float':
            # Determining ranges
            rangeMin, rangeMax = autoRangeDataDict(dataDict,
                    dataRangeMin, dataRangeMax)

        plotLabels = self._chipGroupLabels(chipGroups)

        subchipPatches = []
        for chipLabel in plotLabels:
            if chipLabel in dataDict:

                chipValues = list(dataDict[chipLabel].values())
                if chipValues == []:
                    continue
                    
                if dataType == 'float':
                    chipColors, _, _ = c.floatsColors(chipValues, colormapName,
                        rangeMin, rangeMax, NoneColor,
                        clippingLowColor, clippingHighColor)

                elif dataType == 'string':
                    log.debug(f'[_allChipSubpatches] chipValues: {chipValues}')
                    chipColors, colorDict = c.stringsColors(chipValues, colormapName, NoneColor, colorDict)

                elif dataType == 'bool':
                    raise NotImplementedError('Bool data plotting is not yet implemented.')


                subPatches = self._chipSubPatches(chipLabel, chipColors)
                subchipPatches += subPatches

            
        return subchipPatches, rangeMin, rangeMax, colorDict


    def _allChipPatches(self, dataDict:dict,
        dataType:str,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        colormapName:str = None,
        colorDict:dict = None,
        NoneColor = None,
        clippingLowColor = None,
        clippingHighColor = None):
        """Returns all the chip patches to be plotted by _plot."""
    
        if NoneColor is None: NoneColor = c.DEFAULT_NONE_COLOR
    
        rangeMin, rangeMax = None, None

        if dataType == 'float':
            # Determining ranges
            rangeMin, rangeMax = autoRangeDataDict(dataDict,
                    dataRangeMin, dataRangeMax)
        

        chipLabels = list(dataDict.keys())
        chipValues = list(dataDict.values())
        

        if dataType == 'float':
            chipColors, _, _ = c.floatsColors(chipValues, colormapName,
                rangeMin, rangeMax, NoneColor,
                clippingLowColor, clippingHighColor)
        elif dataType == 'string':
            # log.debug(f'[_allChipSubpatches] chipValues: {chipValues}')
            chipColors, colorDict = c.stringsColors(chipValues,
                colormapName, NoneColor, colorDict)
        elif dataType == 'bool':
            raise NotImplementedError('Bool data plotting is not yet implemented.')


        chipPatches = [self._chipPatch(lab, col)
                    for lab, col in zip (chipLabels, chipColors)]

        return chipPatches, rangeMin, rangeMax, colorDict

    # Mid level plotting methods

    def plotData_chipScale(self, dataDict:dict, title:str = None, *, 
        dataType:str,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        NoneColor = None,
        colorDict:dict = None,
        clippingHighColor = None,
        clippingLowColor = None,
        BackColor = 'White',
        chipGroups:list = None,
        waferName:str = None,
        colormapName:str = None,
        colorbarLabel:str = None,
        printChipLabels = False,
        chipLabelsDirection:str = None,
        dpi = None,
        ):
        """This is the main function used to plot data at chip-scale.

        It is assuemed dataDict is in the form:
        {
            'DR4-01': <data>,
            'DR4-02': <data>,
            ...
        }

        dataType is used to determine how to plot data.

        """

        if not isinstance(dataDict, dict):
            raise TypeError('"dataDict" must be a dictionary.')

        if isinstance(dataRangeMax, int): dataRangeMax = float(dataRangeMax)
        if isinstance(dataRangeMin, int): dataRangeMin = float(dataRangeMin)

         # Generating patches
        plotLabels = self._chipGroupLabels(chipGroups)
        backChipPatches = [self._chipPatch(l, BackColor) for l in plotLabels
                if l not in dataDict]

        chipPatches, rangeMin, rangeMax, colorDict = \
            self._allChipPatches(dataDict, dataType,
                dataRangeMin, dataRangeMax,
                colormapName, colorDict, NoneColor,
                clippingLowColor, clippingHighColor)

        allPatches = [p.waferPatch(self.D, self.notch)]
        allPatches += backChipPatches
        allPatches += chipPatches

        # Plot settings

        if NoneColor is None: NoneColor = c.DEFAULT_NONE_COLOR
            
        legendDict = {'Data n/a': NoneColor}
        if clippingLowColor is not None:
            legendDict['Under-range'] = clippingLowColor
        if clippingHighColor is not None:
            legendDict['Over-range'] = clippingHighColor

        if dataType == 'float':
            printBar = True

        if dataType == 'string':
            printBar = False
            for string, color in colorDict.items():
                legendDict[string] = color
            
        # Plotting
        self._plot(allPatches, rangeMin, rangeMax,
            title = title,
            waferName = waferName,
            legendDict=legendDict,
            printBar = printBar,
            barLabel = colorbarLabel,
            colormapName = colormapName,
            printLabels = printChipLabels,
            labelsDirection = chipLabelsDirection,
            dpi = dpi,
            )


        # print(f'DEBUG: {rangeMin}')
        # print(f'DEBUG: {rangeMax}')

    def plotData_subchipScale(self, dataDict, title:str = None, *,
        dataType:str,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        NoneColor = None,
        colorDict:dict = None,
        clippingHighColor = None,
        clippingLowColor = None,
        BackColor = 'White',
        chipGroups:list = None,
        waferName:str = None,
        colormapName:str = None,
        colorbarLabel:str = None,
        printChipLabels = False,
        chipLabelsDirection:str = None,
        dpi = None):
        """This is the main function used to plot data on a sub-chip scale.
                
        Arguments:
            - dataDict: the dictionary containing the data
            - title (string): the title of the plot
        
        Keyword arguments:
            - dataType (string): the kind of data to be plotted. Can be "float"
                or "string".
            - dataRangeMin (float) [optional]: The minimum value to be plotted.
                Valid for dataType = "float"
            - dataRangeMax (float) [optional]: The maximum value to be plotted.
                Valid for dataType = "float"
            - NoneColor (3-tuple or None): The color used to plot None data
            - clippingHighColor [optional]: color used when data exceed
                dataRangeMax. Valid for dataType = "float"
            - clippingHighColor [optional]: color used when data exceed
                dataRangeMin. Valid for dataType = "float"
            - chipGroups (list of Strings): if passed, it only shows chips for
                the given group.
                For Budapest wafers, allowed values are: "DR4", "DR8", "FR4",
                    "FR8"
            - waferName (string) [optional]: The name of the wafer to be shown
            - colormapName (string) [optional]: a matplotlib colormap name
            - colorbarLabel (string) [optional]: the label of the bar legend.
                Valid for dataType = "float"
            - printChipLabels (bool, def. False): If True, chip labels are
                printed
            - chipLabelsDirection (string | None) [optional]: If "East",
                "West", "North" or "South" it displaces the chip labels to the
                side of the chip.
            - dpi (number): if passed, it changes the dpi setting of matplotlib
        
        Data dict has to be properly formatted, following the pattern:
        {
            <chipLabel>: {'loc1': <data>, 'loc2': <data>, ...},
            ...
        }
        where <chipLabel> must correspond to the chips defined on the wafer
        maskset, while <data> can be a float or None.
        For each chip, the number of location/data pairs must correspond to the
        number of MZs on chip (e.g. DR8 -> 8 location/data pairs).

        If a <chipLabel> is missing, plotData_subchipScale() only shows the
        outer boundary of the chip; pass a dictionary with None values to show
        that data is missing
        e.g. <chipLabel>: {'loc1': None, 'loc2': None, ...},
        """

        if not isinstance(dataDict, dict):
            raise TypeError('"dataDict" must be a dictionary.')

        if isinstance(dataRangeMax, int): dataRangeMax = float(dataRangeMax)
        if isinstance(dataRangeMin, int): dataRangeMin = float(dataRangeMin)

        # Generating patches
        plotLabels = self._chipGroupLabels(chipGroups)
        chipPatches = [self._chipPatch(l, BackColor) for l in plotLabels]

        subchipPatches, rangeMin, rangeMax, colorDict = \
            self._allChipSubpatches(dataDict,
                dataType,
                dataRangeMin, dataRangeMax,
                colormapName,
                colorDict,
                NoneColor,
                clippingLowColor,
                clippingHighColor,
                chipGroups)

        allPatches = [p.waferPatch(self.D, self.notch)]
        allPatches += chipPatches
        allPatches += subchipPatches

        # Plot settings

        if NoneColor is None: NoneColor = c.DEFAULT_NONE_COLOR

        legendDict = {'Data n/a': NoneColor}
        if clippingLowColor is not None:
            legendDict['Under-range'] = clippingLowColor
        if clippingHighColor is not None:
            legendDict['Over-range'] = clippingHighColor

        if dataType == 'float':
            printBar = True

        if dataType == 'string':
            printBar = False
            for string, color in colorDict.items():
                legendDict[string] = color
            
        # Plotting
        img,ax= self._plot(allPatches, rangeMin, rangeMax,
            colormapName,
            title = title,
            chipGroups = chipGroups,
            waferName = waferName,
            legendDict=legendDict,
            printBar = printBar,
            barLabel = colorbarLabel,
            printLabels = printChipLabels,
            labelsDirection = chipLabelsDirection,
            dpi = dpi
            )
        return img


def normalizeFloatData(dataList:list):
    """Converts integer found within dataList to float;
    float and None are left unchanged.
    
    Raises TypeError if dataList is not a list.
    Raises ValueError if elements of dataList are not float, integers or None.
    """

    if not isinstance(dataList, list):
        raise TypeError('"dataList" must be a list of float, integers or None.')

    newData = []
    for data in dataList:
        if isinstance(data, int):
            data = float(data)
        
        if data is not None:
            if not isinstance(data, float):
                raise ValueError('"dataList" must be a list of float, integers or None.')
        
        newData.append(data)

    return newData


def autoRangeDataDict(dataDict:dict,
        userRangeMin:float = None, userRangeMax:float = None):

    if isinstance(userRangeMin, int):
        userRangeMin = float(userRangeMin)

    if isinstance(userRangeMax, int):
        userRangeMax = float(userRangeMax)

    # Determining ranges
    allData = []
    for value in dataDict.values():
        
        if isinstance(value, dict): # subchip-scale
            newValues = list(value.values())
            allData+= newValues
        else: # chip-scale
            allData.append(value)
        
    allData = normalizeFloatData(allData)
    rangeMin, rangeMax = c.autoFloatRanges(allData)

    if userRangeMin is not None:
        rangeMin = userRangeMin
    
    if userRangeMax is not None:
        rangeMax = userRangeMax
    
    if rangeMin > rangeMax:
        return rangeMax, rangeMin
    else:
        return rangeMin, rangeMax



def waferPlotter(connection, maskSet:str):

    blueprintIDs = {
        'Bilbao': '63c045efeb57e74cb519be89', # "Bilbao wafer"
        'Budapest': '6398434725e51a373ac387fb', # "Budapest wafer"
        'Cambridge': '642592f497d3cc3392ab4202', # "Cambridge wafer"
        'Como': None
    }
    masksetString = ', '.join([f'"{m}"' for m in blueprintIDs.keys()])


    if not isinstance(maskSet, str):
        raise TypeError('"maskSet" must be a string.')
    if not maskSet in blueprintIDs.keys():
        raise TypeError(f'"maskSet" must be among {masksetString}.')

    ID = blueprintIDs[maskSet]

    waferBP = importWaferBlueprint(ID, connection)

    return _waferPlotter(connection, waferBP = waferBP)
