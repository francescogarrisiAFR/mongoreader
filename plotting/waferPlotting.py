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
        """Given the chip groups, it returns all the chip labels associated
        to them.

        Elements of "groups" must be among self.allowedGroups.

        Args:
            groups (list[str]): The chip groups.

        Raises:
            TypeError: If arguments are not specified correctly.
            ValueError: If arguments are not specified correctly.

        Returns:
            list[str]: The list of labels. Does NOT return None.
        """

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
        """Returnes the p1-p2 pairs of points that identify chip rectangles on
        wafer.

        Args:
            chipLabel (str): The label that identifies the chip.

        Raises:
            ValueError: If chip label is not in self.allChipLabels.

        Returns:
            2-tuple: p1, p2, where p1 is a (x, y) tuple of floats, representing
                coordinates in microns.
        """        

        if not chipLabel in self.allChipLabels:
            raise ValueError(f'"chipLabel" ("{chipLabel}") is not valid.')
        
        p12 = self.chipP1P2s[chipLabel] # um

        p1 = list(map(lambda x: float(x)/1000, p12["p1"])) # um to mm
        p2 = list(map(lambda x: float(x)/1000, p12["p2"])) # um to mm

        log.debug(f'[_chipP1P2] "{chipLabel}" p1: {p1} - p2: {p2}')

        return p1, p2

    def _chipPatch(self, chipLabel:str, color = None):
        """Returnes the (possibily colored) rectangle patch for a labeled chip.

        Args:
            chipLabel (str): The label that identifies the chip.
            color (str | 3-tuple[float], optional): The color of the rectangle
                patch. Defaults to None.

        Returns:
            mongoreader.patches.rectPatch: The patch for the rectangle.
        """        

        p1, p2 = self._chipP1P2(chipLabel)
        
        return p.rectPatch(p1, p2, color)

    
    def _chipSubPatches(self, chipLabel:str, colors:list):
        """Returns a list of rectangles (possibily colored) that fill the space
        of a chip on the wafer.
        
        The number of sub-sections is determined from the length of the
        "colors" list, which must be passed and cannot be None.
        
        "colors" contains colors (string or 3-tuple) or None.

        Internally the method calls mongoreader.patches.rectSubPatches().

        Args:
            chipLabel (str): The label that identifies the chip.
            colors (list[str|3-tuple|None]): The colors associated to the
                sub-patches.

        Raises:
            TypeError: If arguments are not correctly specified.

        Returns:
            list[mongoreader.patches.rectPatch]: The sub-patches.
        """        
        p1, p2 = self._chipP1P2(chipLabel)
        
        if not isinstance(colors, list):
            raise TypeError('"colors" must be a list, containing float 3-tuples, strings or None.')

        subSections = len(colors)

        log.debug(f'[_chipSubPatches] {chipLabel}: {p1} - {p2}')
        log.debug(f'[_chipSubPatches] subSections: {subSections}')
        log.debug(f'[_chipSubPatches] colors: {colors}')
        
        rects = p.rectSubPatches(p1, p2, subSections, colors)
        return rects


    def _addChipLabelText(self, fig, ax, chipLabel:str, direction:str = None):
        """Adds a chip label as a text element to a figure, close to the chip
        patch.

        Args:
            fig (matplotlib figure): The matplotlib figure (not actually used).
            ax (matplotlib axis): The axes of the plot.
            chipLabel (str): The label that identifies the chip.
            direction (str, optional): If passed, it shifts the label from the
                center of the chip (default position) to just outside the
                border of the chip. It can be either "North", "East", "South",
                or "West". Defaults to None.

        Raises:
            TypeError: If arguments are not specified correctly.
            ValueError: If arguments are not specified correctly.
        """        

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
        """This is the main function used to realize wafer-plots.

        Args:
            patches (list): The list of patches that compose the plot
            rangeMin (float): The minimum range value for the data.
            rangeMax (float): The maximum range value for the data.
            colormapName (str, matplotlib colormap name): The name of the
                colormap used for plotting the data.
            chipGroups (list, optional): If passed, only the chips associated
                to the passed groups are rendered. If not, all the chips are
                rendered. Defaults to None.
            printBar (bool, optional): If True, the lateral colorbar legend is
                plotted. Defaults to True.
            barLabel (str, optional): The label associated to the colorbar.
                Defaults to None.
            legendDict (dict, optional): Used for string-type plots. Defaults
                to None.
            legendFontSize (int, optional): Self-explanatory. Defaults to None.
            title (str, optional): The title string that is put at the top of
                the plot. Defaults to None.
            waferName (str, optional): If passed, it appears at the bottom of
                the wafer frame, under the notch. Defaults to None.
            printDate (bool, optional): If True, the date on which the plot is
                generated is added at the bottom. Defaults to True.
            printLabels (bool, optional): If True, chip labels are added to the
                plot. Defaults to True.
            labelsDirection (str, optional): If passed, chip labels are shifted
                to the side of the chips, depending on the actual value, which
                must be among "North", "East", "South", "West". Defaults to
                None.
            dpi (int, optional): dots per inch of the rendered figure. Defaults
                to None.

        Returns:
            fig, ax: The figure-axis pair of matplotlib figures.
        """        

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


    def _allChipSubpatches(self, dataDict:dict, *,
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
        """Given the data dictionary and dataType arguments, this method
        generates all the sub-patches of the chips to be included in the plot.

        This method is used for subchip-scale data, that is when multiple
        values are associated to each chip. As such, the dataDict is expected
        to be in the form
        >>> {
        >>>     <chipLabel1>: {<value1>, <value2>, ...},
        >>>     <chipLabel2>: {<value1>, <value2>, ...},
        >>>     ...
        >>> }

        Args:
            dataDict (dict): The data dictionary.
            dataType (str): The kind of data in the dictionary (currently 
                "float" or "string").
            dataRangeMin (float, optional): The minimum range value for the
                data. Defaults to None.
            dataRangeMax (float, optional): The maximum range value for the
                data. Defaults to None.
            colormapName (str, matplotlib colormap name): The name of the
                colormap used for plotting the data.
            colorDict (dict, optional): If passed, it specifies the colors
                associated to string data ({<string1>: <color1>, <string2>:
                <color2>, ...}). Defaults to None.
            NoneColor (str|3-tuple[float], optional): The color associated to
                None values in the data dictionary. Defaults to None.
            clippingLowColor (str|3-tuple[float], optional): If passed, values
                below "dataRangeMin" will be rendered with this color,
                otherwise the extreme color of the matplotlib colormap is used.
                Defaults to None.
            clippingHighColor (str|3-tuple[float], optional): If passed, values
                above "dataRangeMax" will be rendered with this color,
                otherwise the extreme color of the matplotlib colormap is used.
                Defaults to None.
            chipGroups (list, optional): If passed, only the chips associated
                to the passed groups are rendered. If not, all the chips are
                rendered. Defaults to None.

        Raises:
            NotImplementedError: If "bool" is passed as dataType.

        Returns:
            4-tuple: subchipPatches, rangeMin, rangeMax, colorDict
        """        

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


    def _allChipPatches(self, dataDict:dict, *,
        dataType:str,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        colormapName:str = None,
        colorDict:dict = None,
        NoneColor = None,
        clippingLowColor = None,
        clippingHighColor = None):
        """Given the data dictionary and dataType arguments, this method
        generates all the patches of the chips to be included in the plot.

        This method is used for chip-scale data, that is, when a single value
        is associated to each chip. As such, the dataDict is expected
        to be in the form
        >>> {
        >>>     <chipLabel1>: <value1>,
        >>>     <chipLabel2>: <value2>,
        >>>     ...
        >>> }

        Args:
            dataDict (dict): The data dictionary,
            dataType (str): The kind of data in the dictionary (currently 
                "float" or "string").
            dataRangeMin (float, optional): The minimum range value for the
                data. Defaults to None.
            dataRangeMax (float, optional): The maximum range value for the
                data. Defaults to None.
            colormapName (str, matplotlib colormap name): The name of the
                colormap used for plotting the data.
            colorDict (dict, optional): If passed, it specifies the colors
                associated to string data ({<string1>: <color1>, <string2>:
                <color2>, ...}). Defaults to None.
            NoneColor (str|3-tuple[float], optional): The color associated to
                None values in the data dictionary. Defaults to None.
            clippingLowColor (str|3-tuple[float], optional): If passed, values
                below "dataRangeMin" will be rendered with this color,
                otherwise the extreme color of the matplotlib colormap is used.
                Defaults to None.
            clippingHighColor (str|3-tuple[float], optional): If passed, values
                above "dataRangeMax" will be rendered with this color,
                otherwise the extreme color of the matplotlib colormap is used.
                Defaults to None.
            
        Raises:
            NotImplementedError: If "bool" is passed as dataType.

        Returns:
            4-tuple: chipPatches, rangeMin, rangeMax, colorDict
        """
    
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
        """This is the main function used to plot data at chip-scale, that is
        when a single value is associated to each chip.

        The arguments can be used to customize the plot in various ways, as
        described below.

        The method expects at least the data dictionary and the dataType
        arguments, which specify the kind of data present in the dictionary.

        The dataDictionary must be in the form:
        >>> {
        >>>     <chipLabel1>: <data1>,
        >>>     <chipLabel1>: <data2>,
        >>>     ...
        >>> }
        where <chipLabel1> must be for instance "DR8-01", "02-SE", etc., while
        <dataX> can be a float, integer, string or None.
        
        In this context None means that the value is not available for that
        chip (and NoneColor is used), while if a chip label is missing from the
        dictionary, the chip is rendered as and empty chip (and BackColor is
        used).
        
        dataType currently must be passed (the method does not recognize
        automatically the data type) and allowed values are "float" and
        "string". Use "float" for integer values.

        Args:
            dataDict (dict): The data dictionary.
            dataType (str): The kind of data in the dictionary (currently 
                "float" or "string").
            title (str, optional): The title string that is put at the top of
                the plot. Defaults to None.
            dataRangeMin (float, optional): The minimum range value for the
                data. Defaults to None.
            dataRangeMax (float, optional): The maximum range value for the
                data. Defaults to None.
            NoneColor (str|3-tuple, optional): The color used for None values.
                Defaults to None.
            colorDict (dict, optional): If passed, it is the dictionary that 
                associates colors to string-type values. If not passed, colors
                are determined automatically form the colormap used. Defaults
                to None.
            clippingLowColor (str|3-tuple[float], optional): If passed, values
                below "dataRangeMin" will be rendered with this color,
                otherwise the extreme color of the matplotlib colormap is used.
                Defaults to None.
            clippingHighColor (str|3-tuple[float], optional): If passed, values
                above "dataRangeMax" will be rendered with this color,
                otherwise the extreme color of the matplotlib colormap is used.
                Defaults to None.
            BackColor (str|3-tuple, optional): The color used to render chips
                that are not present in the data dictionary (relevant when the
                chip groups selected contain chips that are not included in the 
                data dictionary). Defaults to 'White'.
            chipGroups (list, optional): If passed, only the chips associated
                to the passed groups are rendered. If None is passed, all the
                chips are rendered (if an empty list is passed, nothing is
                rendered). Defaults to None.
            waferName (str, optional): If passed, it appears at the bottom of
                the wafer frame, under the notch. Defaults to None.
            colormapName (str, matplotlib colormap name): The name of the
                colormap used for plotting the data.
            colorbarLabel (str, optional): The label associated to the
                colorbar. Defaults to None.
            printChipLabels (bool, optional): If True, chip labels are added to
                the plot. Defaults to False.
            chipLabelsDirection (str, optional): If passed, chip labels are
                shifted to the side of the chips, depending on the actual
                value, which must be among "North", "East", "South", "West".
                Defaults to None.
            dpi (int, optional): dots per inch of the rendered figure. Defaults
                to None.

        Returns:
            fig, ax: The figure-axis pair of matplotlib figures.
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
            self._allChipPatches(dataDict, dataType = dataType,
                dataRangeMin = dataRangeMin,
                dataRangeMax = dataRangeMax,
                colormapName = colormapName,
                colorDict = colorDict,
                NoneColor = NoneColor,
                clippingLowColor = clippingLowColor,
                clippingHighColor = clippingHighColor)

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
        fig, ax = self._plot(allPatches, rangeMin, rangeMax,
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

        return fig, ax
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
        """This is the main function used to plot data at subchip-scale, that
        is when multiple values are associated to each chip.

        The arguments can be used to customize the plot in various ways, as
        described below.

        The method expects at least the data dictionary and the dataType
        arguments, which specify the kind of data present in the dictionary.

        The dataDictionary must be in the form:
        >>> {
        >>>     <chipLabel1>: {
        >>>                 <locationLabel1>: <value1>,
        >>>                 <locationLabel2>: <value2>,
        >>>                 ...
        >>>             },
        >>>     <chipLabel2>: {
        >>>                 <locationLabel1>: <value1>,
        >>>                 <locationLabel2>: <value2>,
        >>>                 ...
        >>>             },
        >>>     ...
        >>> }
        where <chipLabel1> must be for instance "DR8-01", "02-SE", etc.,
        <dataX> can be a float, integer, string or None, and <locationLabelX>
        must be a location label associated to the chip (e.g. "MZ1", "MZ2",
        etc.).

        Allowed location labels are defined in the chip blueprint.
        
        In this context None means that the value is not available for that
        chip (and NoneColor is used), while if a chip label is missing from the
        dictionary, the chip is rendered as and empty chip (and BackColor is
        used). In particular, if a <chipLabel> is missing, this method only
        shows the outer boundary of the chip; pass a dictionary with None
        values to show that data is missing.
        
        dataType currently must be passed (the method does not recognize
        automatically the data type) and allowed values are "float" and
        "string". Use "float" for integer values.
        
        Args:
            dataDict (dict): The data dictionary.
            dataType (str): The kind of data in the dictionary (currently 
                "float" or "string").
            dataRangeMin (float, optional): The minimum range value for the
                data. Defaults to None.
            dataRangeMax (float, optional): The maximum range value for the
                data. Defaults to None.
            NoneColor (str|3-tuple, optional): The color used for None values.
                Defaults to None.
            colorDict (dict, optional): If passed, it is the dictionary that 
                associates colors to string-type values. If not passed, colors
                are determined automatically form the colormap used. Defaults
                to None.
            clippingLowColor (str|3-tuple[float], optional): If passed, values
                below "dataRangeMin" will be rendered with this color,
                otherwise the extreme color of the matplotlib colormap is used.
                Defaults to None.
            clippingHighColor (str|3-tuple[float], optional): If passed, values
                above "dataRangeMax" will be rendered with this color,
                otherwise the extreme color of the matplotlib colormap is used.
                Defaults to None.
            BackColor (str|3-tuple, optional): The color used to render chips
                that are not present in the data dictionary (relevant when the
                chip groups selected contain chips that are not included in the 
                data dictionary). Defaults to 'White'.
            chipGroups (list, optional): If passed, only the chips associated
                to the passed groups are rendered. If None is passed, all the
                chips are rendered (if an empty list is passed, nothing is
                rendered). Defaults to None.
            waferName (str, optional): If passed, it appears at the bottom of
                the wafer frame, under the notch. Defaults to None.
            colormapName (str, matplotlib colormap name): The name of the
                colormap used for plotting the data.
            colorbarLabel (str, optional): The label associated to the
                colorbar. Defaults to None.
            printChipLabels (bool, optional): If True, chip labels are added to
                the plot. Defaults to False.
            chipLabelsDirection (str, optional): If passed, chip labels are
                shifted to the side of the chips, depending on the actual
                value, which must be among "North", "East", "South", "West".
                Defaults to None.
            dpi (int, optional): dots per inch of the rendered figure. Defaults
                to None.
        
        Raises:
            TypeError: If dataDict is not a dictionary.
        
        Returns:
            fig, ax: The figure-axis pair of matplotlib figures.
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
                dataType = dataType,
                dataRangeMin = dataRangeMin, dataRangeMax = dataRangeMax,
                colormapName = colormapName,
                colorDict = colorDict,
                NoneColor = NoneColor,
                clippingLowColor = clippingLowColor,
                clippingHighColor = clippingHighColor,
                chipGroups = chipGroups)

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
        fig, ax=self._plot(allPatches, rangeMin, rangeMax,
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
        return fig, ax


def normalizeFloatData(dataList:list):
    """Converts integer found within dataList to float; float and None are left
    unchanged.

    Args:
        dataList(list): The data list.
    
    Raises:
        TypeError: If dataList is not a list.
        ValueError: If elements of dataList are not float, integers or None.
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
    """Given the dataDict, it returns the max and min values for its range.

    One can overwrite the automatically determined value using the
    userRangeMin and userRangeMax arguments.

    Args:
        dataDict (dict): The data dictionary
        userRangeMin (float, optional): Overwrites the minimum range value.
            Defaults to None.
        userRangeMax (float, optional): Overwrites the maximum range value.
            Defaults to None.

    Returns:
        2-tuple[float]: rangeMin, rangeMax
    """    

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
    """This function is used to return an instance of the correct sub-class
    of the waferPlotter class, based on the maskSet name.

    Internally, the function retrieves the wafer blueprint associated to the
    mask set and uses it to instanciate a _waferPlotter object.

    Args:
        connection (mongomanager.connection): The connection instance to the
            MongoDB server.
        maskSet (str): The name of the mask set to be used. Currently
            "Bilbao" and "Budapest" are supported.

    Raises:
        TypeError: If arguments are not specified correctly.

    Returns:
        _waferPlotter: The correct wafer plotter instance.
    """    

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
