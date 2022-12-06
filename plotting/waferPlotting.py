"""This module contains functions and classes useful to plot data on wafer
shapes"""

import matplotlib.pyplot as plt
import json
import datetime as dt

# from . import patches as p
# from . import colors as c

from mongoreader.plotting import patches as p
from mongoreader.plotting import colors as c

from pathlib import Path
from numpy import random, linspace



class _waferPlotter:
    """This class should not be instantiated directly, but only through its sub-classes"""


    def __init__(self, notch = None, *,
        waferMaskLabel:str,
        allowedGroups:list = None):

        if not isinstance(waferMaskLabel, str):
            raise TypeError('"waferMaskLabel" must be a string.')

        if allowedGroups is not None:
            if not isinstance(allowedGroups, list):
                raise TypeError('"allowedGroups" must be a list of strings or None.')
            
            for el in allowedGroups:
                if not isinstance(el, str):
                    raise ValueError('"allowedGroups" must be a list of strings or None.')

        self.D = 101.6 # Wafer plot diameter in mm
        if notch is None:
            self.notch = 5.0

        self.waferSizes = self._retrieveObjectsSizes(waferMaskLabel)
        self.allowedGroups = allowedGroups

    def _retrieveObjectsSizes(self, waferMaskLabel):

        if waferMaskLabel is None:
            return

        try:
            path = Path(__file__).parent / 'waferSizes.json'
            print(path)
            with open(path, 'r') as file:
                dimensions = json.load(file)

            return dimensions[waferMaskLabel]

        except FileNotFoundError:
            print('Warning: Dimensions file not found!')
            return None

    def _chipGroupLabels(self, groups:list):

        if groups is None:
            return list(self.waferSizes['chips'].keys())

        allowedGroupString = ', '.join([f'"{g}"' for g in self.allowedGroups])

        if not isinstance(groups, list):
                raise TypeError(f'"groups" must be a list of strings among {allowedGroupString}.')

        for el in groups:
            if not el in self.allowedGroups:
                raise ValueError(f'"groups" must be a list of strings among {allowedGroupString}.')

        labels = []
        for group in groups:
            labels += [key for key in self.waferSizes['chips'] if group in key]
        return labels
        
    
    def _chipP1P2(self, chipLabel:str):
        """Returnes the p1-p2 pairs of points that identify chip rectangles on wafer."""

        if not chipLabel in self.waferSizes['chips']:
            raise ValueError(f'"chipLabel" ("{chipLabel}") is not valid.')
        
        p12 = self.waferSizes['chips'][chipLabel]

        p1 = p12["p1"]
        p2 = p12["p2"]

        return p1, p2

    def _chipPatch(self, chipLabel:str, color = None):
        """Returnes the (possibily colored) rectangle patch for a labeled chip."""

        p1, p2 = self._chipP1P2(chipLabel)
        
        return p.rectPatch(p1, p2, color)

    
    def _chipSubPatches(self, chipLabel:str, subSections:int, colors:list = None):
        """Returns a list of rectangles (possibily colored) that fill the space
        of a chip on the wafer.
        
        The number of sub-sections is determined from the kind of chip.
        DR4/FR4 -> 4
        DR8/FR8 -> 4
        
        If "colors" is passed, it must be a list as long as the number of
        subsections. "colors" may contain colors or None.

        """
        p1, p2 = self._chipP1P2(chipLabel)

        rects = p.rectSubPatches(p1, p2, subSections, colors)
        return rects


    def _addChipLabelText(self, fig, ax, chipLabel):

        p1, p2 = self._chipP1P2(chipLabel)
        x0, x1, y0, y1, width, height = p._unpackP12(p1, p2)

        textx = x0+width/2
        texty = y0+height/2

        ax.text(textx, texty, chipLabel, fontsize = 10,
            ha = 'center',
            va = 'center')


    # Plot methods

    def _plot(self, patches:list,
            rangeMin, rangeMax,
            colormapName,
            barLabel:str = None,
            title:str = None,
            waferName:str = None,
            
            printDate:bool = True,
            printLabels:bool = True):
        """This is the main function used to realize wafer-plots."""

        plt.figure(dpi = 200, figsize = (10, 10))
        fig, ax = plt.subplots() # note we must use plt.subplots, not plt.subplot
        fig.set_size_inches(10, 10, forward=True)
        
        for ptc in patches:
            ax.add_patch(ptc)
        
        # Chip labels:
        if printLabels:
            for label in self.waferSizes['chips'].keys():
                self._addChipLabelText(fig, ax, label)

        # Wafer name
        if waferName is not None:
            ax.text(0, -self.D/2, waferName,
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

        c.addColorBar(fig, ax, barLabel, rangeMin, rangeMax, colormapName)

        plt.show()

        return fig, ax


    # Mid level plotting methods

    def plotData_chipScale(self, dataDict:dict, title:str = None, *, 
        dataType:str,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        NoneColor = None,
        chipGroups:list = None,
        waferName:str = None,
        colormapName:str = None,
        colorbarLabel:str = None):
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

        plotLabels = self._chipGroupLabels(chipGroups)

        if dataType == 'float':

            # for label in plotLabels:
            values = [dataDict[key] if key in dataDict else None
                            for key in plotLabels]

            colors, rangeMin, rangeMax = c.floatsColors(values, colormapName, dataRangeMin, dataRangeMax, NoneColor)
            colorsDict = {key: col for key, col in zip(plotLabels, colors)}

            chipPatches = [self._chipPatch(l, c) for l, c in colorsDict.items()]

        allPatches = [p.waferPatch(self.D, self.notch)]
        allPatches += chipPatches

        self._plot(allPatches, rangeMin, rangeMax,
            colormapName,
            title = title,
            waferName = waferName,
            barLabel = colorbarLabel,
            )

        # print(f'DEBUG: {rangeMin}')
        # print(f'DEBUG: {rangeMax}')
        # fig, ax = c.addColorBar(fig, ax, rangeMin, rangeMax, colormapName)


    def plotData_subchipScale(self, dataDict, title:str = None, *,
        dataType:str,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        NoneColor = None,
        chipGroups:list = None,
        waferName:str = None,
        colormapName:str = None,
        colorbarLabel:str = None):

        if not isinstance(dataDict, dict):
            raise TypeError('"dataDict" must be a dictionary.')

        plotLabels = self._chipGroupLabels(chipGroups)


        chipPatches = [self._chipPatch(l, 'White') for l in plotLabels]
        subchipPatches = []

        # Determining ranges
        allData = []
        for dic in dataDict.values():
            allData+= list(dic.values())

        rangeMin, rangeMax = c.autoFloatRanges(allData)

        if dataRangeMin is not None:
            rangeMin = dataRangeMin

        if dataRangeMax is not None:
            rangeMax = dataRangeMax

        if dataType == 'float':
            for chipLabel in plotLabels:
                if chipLabel in dataDict:

                    chipValues = list(dataDict[chipLabel].values())

                    if chipValues == []:
                        continue
                        
                    chipColors, _, _ = c.floatsColors(chipValues, colormapName, rangeMin, rangeMax, NoneColor)
                    subPatches = self._chipSubPatches(chipLabel, chipColors)
                    subchipPatches += subPatches

                else:
                    pass

        allPatches = [p.waferPatch(self.D, self.notch)]
        allPatches += chipPatches
        allPatches += subchipPatches

        self._plot(allPatches, rangeMin, rangeMax,
            colormapName,
            title = title,
            waferName = waferName,
            barLabel = colorbarLabel,
            printLabels = False
            )


class waferPlotter_PAM4(_waferPlotter):

    def __init__(self):

        super().__init__(waferMaskLabel='Bilbao', allowedGroups=['DR4', 'DR8', 'FR4', 'FR8'])
    
    def _chipSubPatches(self, chipLabel:str, colors:list = None):
        """Returns a list of rectangles (possibily colored) that fill the space
        of a chip on the wafer.
        
        The number of sub-sections is determined from the kind of chip.
        DR4/FR4 -> 4
        DR8/FR8 -> 4
        
        If "colors" is passed, it must be a list as long as the number of
        subsections. "colors" may contain colors or None.

        """
        
        subSections = int(chipLabel[2]) # FR8-XX -> 8
        return super()._chipSubPatches(chipLabel, subSections, colors)