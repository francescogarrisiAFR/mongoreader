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



def _joinListsOrNone(*args):
    returnList = []

    for arg in args:
        if arg is None:
            continue
        elif not isinstance(arg, list):
            raise TypeError('All arguments must be lists or None.')
        else:
            returnList += arg

    return returnList

def _joinDictsOrNone(*args):
    returnDict = {}

    for arg in args:
        if arg is None:
            continue
        elif not isinstance(arg, dict):
            raise TypeError('All arguments must be dictionaries or None.')
        else:
            returnDict |= arg

    return returnDict


class _waferPlotter:
    """This class should not be instantiated directly, but only through its sub-classes"""


    def __init__(self, connection, notch = None, *,
        waferBP: waferBlueprint):

        if not isinstance(waferBP, waferBlueprint):
            raise TypeError('"waferBP" must be a mongomanager.waferBlueprint object.')
        
        with opened(connection):
            self.waferBP = waferBP

            # lists, None if not found
            self.allowedChipGroups = waferBP.ChipBlueprints.retrieveGroupNames()
            self.allowedTestChipGroups = waferBP.TestChipBlueprints.retrieveGroupNames()
            self.allowedTestCellGroups = waferBP.TestCellBlueprints.retrieveGroupNames()
            self.allowedBarGroups = waferBP.BarBlueprints.retrieveGroupNames()

            # A dictionary containing all the chipType -> allowed groups relations
            self.allowedGroupsDict = {
                'chips': self.allowedChipGroups,
                'testChips': self.allowedTestChipGroups,
                'testCells': self.allowedTestCellGroups,
                'bars': self.allowedBarGroups,
            }

            self.allChipGroups = _joinListsOrNone(
                self.allowedChipGroups,
                self.allowedTestChipGroups,
            )
            self.chipCellGroups = _joinListsOrNone(
                self.allowedChipGroups,
                self.allowedTestChipGroups,
                self.allowedTestCellGroups,
            )
            self.allGroups = _joinListsOrNone(
                self.allowedChipGroups,
                self.allowedTestChipGroups,
                self.allowedTestCellGroups,
                self.allowedBarGroups
            )

            # lists, None if not found
            self.chipLabels = waferBP.ChipBlueprints.retrieveLabels()
            self.testChipLabels = waferBP.TestChipBlueprints.retrieveLabels()
            self.testCellLabels = waferBP.TestCellBlueprints.retrieveLabels()
            self.barLabels = waferBP.BarBlueprints.retrieveLabels()

            self.allChipLabels = _joinListsOrNone(
                self.chipLabels,
                self.testChipLabels,
            )
            self.chipCellLabels = _joinListsOrNone(
                self.chipLabels,
                self.testChipLabels,
                self.testCellLabels,
            )
            self.allLabels = _joinListsOrNone(
                self.chipLabels,
                self.testChipLabels,
                self.testCellLabels,
                self.barLabels,
            )

            # A dictionary containing all the chipType -> labels relations
            self.labelsDict = {
                'chips': self.chipLabels,
                'testChips': self.testChipLabels,
                'testCells': self.testCellLabels,
                'bars': self.barLabels,
            }

            # A dictionaries containing all the group -> labels relations
            
            self.groupToLabelsDict = _joinDictsOrNone(
                waferBP.ChipBlueprints.retrieveGroupsDict(),
                waferBP.TestChipBlueprints.retrieveGroupsDict(),
                waferBP.TestCellBlueprints.retrieveGroupsDict(),
                waferBP.BarBlueprints.retrieveGroupsDict()
            )

            # dicts, None if not found
            self.chipP1P2s = waferBP.retrieveChipP1P2s(connection, verbose = False)
            self.testChipP1P2s = waferBP.retrieveTestChipP1P2s(connection, verbose = False)
            self.testCellP1P2s = waferBP.retrieveTestCellP1P2s(connection, verbose = False)
            self.barsP1P2s = waferBP.retrieveBarP1P2s(connection, verbose = False)

            self.allP1P2s = _joinDictsOrNone(
                self.chipP1P2s,
                self.testChipP1P2s,
                self.testCellP1P2s,
                self.barsP1P2s,
            )
            

        # Should be retrieved from waferBP's "geometry" field.
        self.D = 101.6 # Wafer plot diameter in mm
        if notch is None:
            self.notch = 2.7

        

    def _plotLabels(self, chipTypes:list = None, groupsDict:list = None):
        """This functions returns the component labels to be used for the
        plot, filtered as specified by chipTypes and groupsDict.

        chipTypes selects the macro-groups defined by chip types ("chips",
        "testChips", "testCells" and "bars").

        groupsDict is used to select given groups for each of the chip types,
        and is in the form:
        {
            <chipType1>: <list of groups | None>,
            <chipType2>: <list of groups | None>,
        }

        All the keys of groupsDict must be present in chipTypes. All their
        values must be present in self.allowedGroupsDict[<chipType>]. If the
        value is set to None, all the labels for that given chip type are
        retrieved.

        Args:
            chipTypes (list[str], optional): The macro-groups for which to
                retrieve labels. Defaults to None, in which case all the wafer
                labels are returned.
            groupsDict (list, optional): Dictionary that filters the retrieved
                labels coording to the groups for the given chip-type
                Defaults to None.

        Returns:
            list[str]: The labels to be used for the plot
        """        

        if chipTypes is None:
            return self.allLabels
                
        # Type checks
        allowedTypes = list(self.allowedGroupsDict.keys())
        
        if not isinstance(chipTypes, list):
            raise TypeError(f'"chipTypes" must be a list of strings among {allowedTypes} or None.')
        if not all([el in allowedTypes for el in chipTypes]):
            raise TypeError(f'"chipTypes" must be a list of strings among {allowedTypes} or None.')

        if groupsDict is not None:
            if not all([key in allowedTypes for key in groupsDict]):
                raise ValueError(f'The keys of "groupsDict" must be among {allowedTypes}.')
            
            if not all([key in chipTypes for key in groupsDict]):
                raise ValueError(f'The keys of "groupsDict" must be among "chipTypes" ({chipTypes}).')

            for ctype, groups in groupsDict.items():
                if groups is not None:
                    if not all([g in self.allowedGroupsDict[ctype] for g in groups]):
                        raise TypeError(f'Values of "groupsDict" must correspond to those among self.allowedGroupsDict.')
        
        # Normalization
        if groupsDict is None:
            groupsDict = {ctype: None for ctype in chipTypes}
        
        plotLabels = []

        for chipType, groups in groupsDict.items():
            plotLabels += self._chipGroupLabels(chipType, groups)

        return plotLabels
    


    def _chipGroupLabels(self, chipType:str, groups:list):
        """Given the chip type and associated groups, it returns all the chip
        labels associated to them.

        Elements of "groups" must be among self.allowed[ChipType]Groups.

        Args:
            groups (list[str]): The chip groups.

        Raises:
            TypeError: If arguments are not specified correctly.
            ValueError: If arguments are not specified correctly.

        Returns:
            list[str]: The list of labels. Does NOT return None.
        """

        # Type checks are performed by _plotLabels()

        # Early exit
        if groups is None:
            log.debug(f'[_chipGroupLabels] Returning all labels for chip type {chipType}: {self.labelsDict[chipType]}')
            return self.labelsDict[chipType]

        
        # Labels
        labels = []
        for group in groups:
            newLabels = self.groupToLabelsDict[group]
            labels += newLabels
        return labels
        
    
    def _cmpP1P2(self, cmpLabel:str):
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

        # log.debug(f'[_chipP1P2] cmpLabel: "{cmpLabel}".')

        if not cmpLabel in self.allLabels:
            raise ValueError(f'"chipLabel" ("{cmpLabel}") is not valid.')
        
        p12 = self.allP1P2s[cmpLabel] # um

        p1 = list(map(lambda x: float(x)/1000, p12["p1"])) # um to mm
        p2 = list(map(lambda x: float(x)/1000, p12["p2"])) # um to mm

        # log.debug(f'[_chipP1P2] "{cmpLabel}" p1: {p1} - p2: {p2}')

        return p1, p2


    def _cmpPatch(self, cmpLabel:str, color = None):
        """Returnes the (possibily colored) rectangle patch for a labeled chip.

        Args:
            chipLabel (str): The label that identifies the chip.
            color (str | 3-tuple[float], optional): The color of the rectangle
                patch. Defaults to None.

        Returns:
            mongoreader.patches.rectPatch: The patch for the rectangle.
        """        

        # log.debug(f'[_chipPatch] chipLabel: "{chipLabel}".')

        p1, p2 = self._cmpP1P2(cmpLabel)
        
        return p.rectPatch(p1, p2, color)


    def _chipSubPatches(self, cmpLabel:str, colors:list):
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
        p1, p2 = self._cmpP1P2(cmpLabel)
        
        if not isinstance(colors, list):
            raise TypeError('"colors" must be a list, containing float 3-tuples, strings or None.')

        subSections = len(colors)

        log.debug(f'[_chipSubPatches] {cmpLabel}: {p1} - {p2}')
        log.debug(f'[_chipSubPatches] subSections: {subSections}')
        log.debug(f'[_chipSubPatches] colors: {colors}')
        
        rects = p.rectSubPatches(p1, p2, subSections, colors)
        return rects


    def _addCmpLabelText(self, fig, ax, cmpLabel:str, direction:str = None):
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

        p1, p2 = self._cmpP1P2(cmpLabel)
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

        ax.text(textx, texty, cmpLabel, fontsize = 10,
            ha = ha,
            va = va)


    # Plot methods

    def _plot(self, patches:list,
            rangeMin, rangeMax,
            colormapName,
            *,
            printBar:bool = True,
            barLabel:str = None,
            legendDict:dict = None,
            legendFontSize = None,
            title:str = None,
            waferName:str = None,
            printDate:bool = True,
            printLabels:bool = True,
            labelsToPrint:list = None,
            labelsDirection:str = None,
            dpi = None
            ):
        """This is the main function used to realize wafer-plots.

        groupsDict is used to select given groups for each of the chip types,
        and is in the form:
        {
            <chipType1>: <list of groups | None>,
            <chipType2>: <list of groups | None>,
            ...
        }
        If a value is set to None, all the groups are automatically retrieved.

        Args:
            patches (list): The list of patches that compose the plot
            rangeMin (float): The minimum range value for the data.
            rangeMax (float): The maximum range value for the data.
            colormapName (str, matplotlib colormap name): The name of the
                colormap used for plotting the data.
            
        Keyword Args:
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
            labelsToPrint (list[str], optional): The labels added to the plot
                when printLabels is True.
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

            if labelsToPrint is None:
                log.warning('printLabels = True, but labelsToPrint is None.')
            else:
                for label in labelsToPrint:
                    self._addCmpLabelText(fig, ax, label, direction = labelsDirection)

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

    
    def _allCmpSubpatches(self, dataDict:dict, *,
        dataType:str,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        colormapName:str = None,
        colorDict:dict = None,
        NoneColor = None,
        wrongDataTypeColor = None,
        clippingLowColor = None,
        clippingHighColor = None,
        TrueColor = None,
        FalseColor = None,
        ):
        """Given the data dictionary and dataType arguments, this method
        generates all the sub-patches of the chips to be included in the plot.

        This method is used for subchip-scale data, that is when multiple
        values are associated to each chip. As such, the dataDict is expected
        to be in the form
        >>> {
        >>>     <label1>: {<value1>, <value2>, ...},
        >>>     <label2>: {<value1>, <value2>, ...},
        >>>     ...
        >>> }
        where each label can refer to any type of chip.

        Args:
            dataDict (dict): The data dictionary.

        Keyword Args:
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
            chipTypes (list[str], optional): If passed, only patches
                correspondings to components in these macro-groups are plotted.
                Defaults to ['chips'].

        Raises:
            NotImplementedError: If "bool" is passed as dataType.

        Returns:
            4-tuple: subchipPatches, rangeMin, rangeMax, colorDict
        """        

        if NoneColor is None: NoneColor = c.DEFAULT_NONE_COLOR
        if wrongDataTypeColor is None: wrongDataTypeColor = c.DEFAULT_WRONGDATATYPE_COLOR
        
        rangeMin, rangeMax = None, None

        if dataType is None: dataType = self._determineDataType(dataDict)

        if dataType == None: # All values are None -> I plot it as string
            dataType = 'string'

        if dataType == 'float':
            # Determining ranges
            rangeMin, rangeMax = autoRangeDataDict(dataDict,
                    dataRangeMin, dataRangeMax)


        subchipPatches = []
        for chipLabel in dataDict:
            if chipLabel in dataDict:

                chipValues = list(dataDict[chipLabel].values())
                if chipValues == []:
                    continue
                    
                if dataType == 'float':
                    chipColors, _, _ = c.floatsColors(chipValues, colormapName,
                        rangeMin, rangeMax, NoneColor, wrongDataTypeColor,
                        clippingLowColor, clippingHighColor)

                elif dataType == 'string':
                    log.debug(f'[_allChipSubpatches] chipValues: {chipValues}')
                    chipColors, colorDict = c.stringsColors(chipValues, colormapName, NoneColor, colorDict)

                elif dataType == 'bool':
                    chipColors, colorDict = c.boolsColor(chipValues, TrueColor, FalseColor,
                        NoneColor, wrongDataTypeColor)


                subPatches = self._chipSubPatches(chipLabel, chipColors)
                subchipPatches += subPatches

            
        return subchipPatches, rangeMin, rangeMax, colorDict

    @staticmethod
    def _determineDataType(dataDict:dict):

        # First, I collect all the values in the dataDict, depending on whether
        # it is chip-scale or subchip-scale

        if all([isinstance(v, dict) for v in dataDict.values()]):
            # Sub-chip scale

            values = []
            for dic in dataDict.values():
                values += list(dic.values())
        
        elif any([isinstance(v, dict) for v in dataDict.values()]):
            # Only some values are dictionaries

            raise TypeError(f'"dataDict" is not specified correctly: some but not all inner values are dictionaries.')
        
        else:
            # Chip-scale
            values = list(dataDict.values())
        
        if all([v is None for v in values]):
            return None

        elif all([v is None or isinstance(v, str) for v in values]):
            return "string"
        
        elif all([v is None or isinstance(v, bool) for v in values]):
            return "boolean"
        
        elif all([v is None or isinstance(v, int) or isinstance(v, float) for v in values]):
            return "float"
        
        else:
            raise ValueError(f'Data type of dataDict could not be determined. Either mixed data types are present, or types other than the ones allowed (string, bool, float/int, None).')

    def _allCmpPatches(self, dataDict:dict, *,
        dataType:str = None,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        colormapName:str = None,
        colorDict:dict = None,
        NoneColor = None,
        wrongDataTypeColor = None,
        clippingLowColor = None,
        clippingHighColor = None,
        TrueColor = None,
        FalseColor = None):
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

        Keyword Args:
            dataType (str, optional): The kind of data in the dictionary
                (currently "float" or "string"). If None, the dataType is
                autodetermined.
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
                None values in the data dictionary. Defaults to
                .colors.NONE_COLOR.
            wrongDataTypeColor (str|3-tuple[float], optional): The color
                associated to values that are not consistent with dataType.
                Defaults to .colors.DEFAULT_WRONGDATATYPE_COLOR
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
        if wrongDataTypeColor is None: wrongDataTypeColor = c.DEFAULT_WRONGDATATYPE_COLOR
    
        rangeMin, rangeMax, colorDict = None, None, None

        log.debug(f'[_allChipSubpatches] dataDict: {dataDict}')
        

        if dataType is None: dataType = self._determineDataType(dataDict)


        if dataType == None: # All values are None -> I plot it as string
            dataType = 'string'

        if dataType == 'float':
            # Determining ranges
            rangeMin, rangeMax = autoRangeDataDict(dataDict,
                    dataRangeMin, dataRangeMax)
        

        chipLabels = list(dataDict.keys())
        log.debug(f'[_allChipSubpatches] chipLabels: {chipLabels}')
        chipValues = list(dataDict.values())
        log.debug(f'[_allChipSubpatches] chipValues: {chipValues}')


        if dataType == 'float':
            chipColors, rangeMin, rangeMax = c.floatsColors(chipValues, colormapName,
                rangeMin, rangeMax, NoneColor, wrongDataTypeColor,
                clippingLowColor, clippingHighColor)
        elif dataType == 'string':
            # log.debug(f'[_allChipSubpatches] chipValues: {chipValues}')
            chipColors, colorDict = c.stringsColors(chipValues,
                colormapName, NoneColor, wrongDataTypeColor, colorDict)
        elif dataType == 'bool':
            chipColors, colorDict = c.boolsColor(chipValues, TrueColor, FalseColor,
                NoneColor, wrongDataTypeColor)

        chipPatches = [self._cmpPatch(lab, col)
                    for lab, col in zip (chipLabels, chipColors)]

        return chipPatches, rangeMin, rangeMax, colorDict

    # Mid level plotting methods

    def plotData_chipScale(self, dataDict:dict, title:str = None, *, 
        dataType:str = None,
        chipTypes:str = None,
        chipGroupsDict:dict = None,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        NoneColor = None,
        wrongDataTypeColor = None,
        colorDict:dict = None,
        clippingHighColor = None,
        clippingLowColor = None,
        TrueColor = None,
        FalseColor = None,
        BackColor = 'White',
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
        described below..

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
        
        "chipTypes" and "chipGroupsDict" can be used to filter which data is to
        be plotted.

        chipTypes is a list of strings among "chips", "testChips", "bars" and
        "testCells", which selects which macro-groups are to be plotted.

        chipGroupsDict is a dictionary in the form
        {
            <chipType1>: [<group1>, <group2>, ...] | None,
            <chipType2>: [<group1>, <group2>, ...] | None,
            ...
        }
        which, for each <chipType>, selects which groups are to be plotted.
        If a value of chipGroupsDict is set to None, all the groups are
        automatically retrieved for that chipType.

        Args:
            dataDict (dict): The data dictionary.
        
        Keyword Args:
            dataType (str, optional): The kind of data in the dictionary
                (currently "float", "string" or "bool"). Defaults to None, in
                which case the type is automatically determined.
            chipTypes (list[str], optional): If passed, only components of a
                given type will be plotted. Defaults to None.
            chipGroupsDict (dict, optional): If passed, determines which groups
                are to be plotted. If not, all the chips are rendered. The
                structure is described above. Defaults to None.
            title (str, optional): The title string that is put at the top of
                the plot. Defaults to None.
            dataRangeMin (float, optional): The minimum range value for the
                data. Defaults to None.
            dataRangeMax (float, optional): The maximum range value for the
                data. Defaults to None.
            NoneColor (str|3-tuple, optional): The color used for None values.
                Defaults to None.
            wrongDataTypeColor (str|3-tuple, optional): The color used for 
                values whose type does not match dataType or None.
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
            TrueColor (str|3-tuple[float], optional): The color assigned to
                True bool values.
            FalseColor (str|3-tuple[float], optional): The color assigned to
                False bool values.
            BackColor (str|3-tuple, optional): The color used to render chips
                that are not present in the data dictionary (relevant when the
                chip groups selected contain chips that are not included in the 
                data dictionary). Defaults to 'White'.
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

        log.debug(f'[plotData_chipScale] labels: {list(dataDict.keys())}')

        if isinstance(dataRangeMax, int): dataRangeMax = float(dataRangeMax)
        if isinstance(dataRangeMin, int): dataRangeMin = float(dataRangeMin)

        if NoneColor is None: NoneColor = c.DEFAULT_NONE_COLOR
        if wrongDataTypeColor is None: wrongDataTypeColor = c.DEFAULT_WRONGDATATYPE_COLOR

        # Filtering dataDict

        if chipTypes is not None:
            plotLabels = self._plotLabels(chipTypes, chipGroupsDict)
            dataDict = {k: v for k, v in dataDict.items() if k in plotLabels}

            backChipPatches = [self._chipPatch(l, BackColor) for l in plotLabels
                if l not in dataDict]
        else:
            backChipPatches = []

        # Type determination
        
        if dataType is None:
            dataType = self._determineDataType(dataDict)

        # Building up patches

        chipPatches, rangeMin, rangeMax, colorDict = \
            self._allCmpPatches(dataDict, dataType = dataType,
                dataRangeMin = dataRangeMin,
                dataRangeMax = dataRangeMax,
                colormapName = colormapName,
                colorDict = colorDict,
                NoneColor = NoneColor,
                wrongDataTypeColor = wrongDataTypeColor,
                clippingLowColor = clippingLowColor,
                clippingHighColor = clippingHighColor,
                TrueColor = TrueColor,
                FalseColor = FalseColor)

        allPatches = [p.waferPatch(self.D, self.notch)]
        allPatches += backChipPatches
        allPatches += chipPatches

        # Plot settings
            
        if printChipLabels:
            if chipTypes is not None:
                labelsToPrint = plotLabels
            else:
                labelsToPrint = list(dataDict.keys())
        else:
            labelsToPrint = None

        legendDict = {'Data n/a': NoneColor, 'Wrong data type': wrongDataTypeColor}
        if clippingLowColor is not None:
            legendDict['Under-range'] = clippingLowColor
        if clippingHighColor is not None:
            legendDict['Over-range'] = clippingHighColor

        if dataType == None:
            printBar = False

        if dataType == 'float':
            printBar = True

        if dataType == 'string':
            printBar = False
            for string, color in colorDict.items():
                legendDict[string] = color

        if dataType == 'bool':
            printBar = False
            for boolean, color in colorDict.items():
                legendDict[str(boolean)] = color
            
        # Plotting
        fig, ax = self._plot(allPatches, rangeMin, rangeMax,
            title = title,
            waferName = waferName,
            legendDict=legendDict,
            printBar = printBar,
            barLabel = colorbarLabel,
            colormapName = colormapName,
            printLabels = printChipLabels,
            labelsToPrint = labelsToPrint,
            labelsDirection = chipLabelsDirection,
            dpi = dpi,
            )

        return fig, ax


    def plotData_subchipScale(self, dataDict, title:str = None, *,
        dataType:str = None,
        chipTypes:str = None,
        chipGroupsDict:dict = None,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        NoneColor = None,
        colorDict:dict = None,
        wrongDataTypeColor = None,
        clippingHighColor = None,
        clippingLowColor = None,
        TrueColor = None,
        FalseColor = None,
        BackColor = 'White',
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
        
        "chipTypes" and "chipGroupsDict" can be used to filter which data is to
        be plotted.

        chipTypes is a list of strings among "chips", "testChips", "bars" and
        "testCells", which selects which macro-groups are to be plotted.

        chipGroupsDict is a dictionary in the form
        {
            <chipType1>: [<group1>, <group2>, ...] | None,
            <chipType2>: [<group1>, <group2>, ...] | None,
            ...
        }
        which, for each <chipType>, selects which groups are to be plotted.
        If a value of chipGroupsDict is set to None, all the groups are
        automatically retrieved for that chipType.
        
        Args:
            dataDict (dict): The data dictionary.

        Keyword Args:
            dataType (str): The kind of data in the dictionary (currently 
                "float" or "string").
            chipTypes (list[str], optional): If passed, only components of a
                given type will be plotted. Defaults to None.
            chipGroupsDict (dict, optional): If passed, determines which groups
                are to be plotted. If not, all the chips are rendered. The
                structure is described above. Defaults to None.
            dataRangeMin (float, optional): The minimum range value for the
                data. Defaults to None.
            dataRangeMax (float, optional): The maximum range value for the
                data. Defaults to None.
            NoneColor (str|3-tuple, optional): The color used for None values.
                Defaults to None.
            wrongDataTypeColor (str|3-tuple, optional): The color used for 
                values whose type does not match dataType or None.
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
            TrueColor (str|3-tuple[float], optional): The color assigned to
                True bool values.
            FalseColor (str|3-tuple[float], optional): The color assigned to
                False bool values.
            BackColor (str|3-tuple, optional): The color used to render chips
                that are not present in the data dictionary (relevant when the
                chip groups selected contain chips that are not included in the 
                data dictionary). Defaults to 'White'.
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

        if NoneColor is None: NoneColor = c.DEFAULT_NONE_COLOR
        if wrongDataTypeColor is None: wrongDataTypeColor = c.DEFAULT_WRONGDATATYPE_COLOR

        # Filtering dataDict

        if chipTypes is not None:
            plotLabels = self._plotLabels(chipTypes, chipGroupsDict)
            dataDict = {k: v for k, v in dataDict.items() if k in plotLabels}

            backChipPatches = [self._chipPatch(l, BackColor) for l in plotLabels
                if l not in dataDict]
        else:
            backChipPatches = []

        # Type determination

        if dataType is None:
            dataType = self._determineDataType(dataDict)

        subchipPatches, rangeMin, rangeMax, colorDict = \
            self._allCmpSubpatches(dataDict,
                dataType = dataType,
                dataRangeMin = dataRangeMin, dataRangeMax = dataRangeMax,
                colormapName = colormapName,
                colorDict = colorDict,
                NoneColor = NoneColor,
                wrongDataTypeColor = wrongDataTypeColor,
                clippingLowColor = clippingLowColor,
                clippingHighColor = clippingHighColor,
                TrueColor = TrueColor,
                FalseColor = FalseColor)

        allPatches = [p.waferPatch(self.D, self.notch)]
        allPatches += backChipPatches
        allPatches += subchipPatches

        # Plot settings

        if printChipLabels:
            if chipTypes is not None:
                labelsToPrint = plotLabels
            else:
                labelsToPrint = list(dataDict.keys())
        else:
            labelsToPrint = None

        legendDict = {'Data n/a': NoneColor}
        if clippingLowColor is not None:
            legendDict['Under-range'] = clippingLowColor
        if clippingHighColor is not None:
            legendDict['Over-range'] = clippingHighColor

        if dataType == None:
            printBar = False

        if dataType == 'float':
            printBar = True

        if dataType == 'string':
            printBar = False
            for string, color in colorDict.items():
                legendDict[string] = color
        
        if dataType == 'bool':
            printBar = False
            for boolean, color in colorDict.items():
                legendDict[str(boolean)] = color
        
        log.debug(f'[plotData_subchipScale] dataType: {dataType}')

        # Plotting
        fig, ax=self._plot(allPatches, rangeMin, rangeMax,
            colormapName,
            title = title,
            waferName = waferName,
            legendDict=legendDict,
            printBar = printBar,
            barLabel = colorbarLabel,
            printLabels = printChipLabels,
            labelsToPrint = labelsToPrint,
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



def waferPlotter(connection, blueprint_orID_orName):
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

    if isinstance(blueprint_orID_orName, waferBlueprint):
        waferBP = blueprint_orID_orName
    elif isinstance(blueprint_orID_orName, str):
        waferBP = waferBlueprint.queryOneByName(connection, blueprint_orID_orName)
    elif isID(blueprint_orID_orName):
        waferBP = importWaferBlueprint(waferBP, connection)
    else:
        raise TypeError('"blueprint_orID_orName" must be the wafer blueprint, or its name, or its ID.')

    return _waferPlotter(connection, waferBP = waferBP)
