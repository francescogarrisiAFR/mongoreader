import mongomanager as mom
from mongomanager import log
from mongomanager.goggleFunctions import componentGoggleFunctions as cmpGoggles
from mongomanager.errors import isListOfStrings
# import mongomanager.info as i
import mongoreader.core as c
import mongoreader.errors as e
import mongoreader.plotting.waferPlotting as wplt

from datautils import dataClass

from mongoutils import isID
from mongoutils.connections import opened


from mongomanager.errors import DocumentNotFound

from numpy import average
from pandas import DataFrame, concat


class _attributeClass:
    def __init__(self, obj):
        self._obj = obj

    @classmethod
    def _attributeName(cls):
        return cls.__name__.lstrip('_')

def _attributeClassDecoratorMaker(attributeClass):

    assert issubclass(attributeClass, _attributeClass), '[_classAttributeDecorator] Not an _attributeClass.'

    def decorator(objClass):

        oldInit = getattr(objClass, '__init__')

        def newInit(self, *args, **kwargs):
            oldInit(self, *args, **kwargs)
            setattr(self, attributeClass._attributeName(), attributeClass(self))

        setattr(objClass, '__init__', newInit)
        
        return objClass

    return decorator

class _Datasheets(_attributeClass):
    """Attribute class to apply Datasheet methods to wafer collations"""
    

    def printHelpInfo(self):
        raise NotImplementedError('Not yet implemented.')

    def retrieveData(self,
                    chipTypes:list = None,
                    chipGroupsDict:dict = None,
                    resultNames:list = None,
                    requiredTags:list = None,
                    tagsToExclude:list = None,
                    locations:list = None,
                    *,
                    returnDataFrame:bool = False,
                    datasheetIndex:int = None,
                ):
        """This method can be used to retrieve data from datasheets defined
        for the components of the wafer collation.

        The argumetns can be used to change what results are collected, as
        described below.

        Args:
            chipTypes (List[str], optional): Pass a list containing any of the
                following to select which wafer components are considered:
                "chips", "testChips", "bars", "testCells".
                Defaults to ["chips"].
            chipGroupsDict (dict, optional): If passed, it can be used to filter
                which group for each chipType is plotted. Pass it in the form
                {
                    <chipType1>: <list of groups for chipType1>,
                    <chipType2>: <list of groups for chipType2>,
                    ...
                }
                Not all chip type must be present within the dictionary. If
                they are not, all the chips for that group are considered.
            resultNames (list, optional): If passed, results whose name is not
                listed here are ignored.
            requiredTags (list, optional): If passed, results tags must contain
                those listed here to be collected.
            tagsToExclude (list, optional): If passed, results whose tags are
                among these are not collected. Defaults to None.
            locations (list, optional): If passed, the result location must be
                among these for it to be collected. Defaults to None.

        Keyword arguments (**kwargs):
            returnDataFrame (bool, optional): If True, results are returned
                as a pandas DataFrame instead of a list of dictionaries.
                Defaults to False.
            datasheetIndex (int, optional): If passed, the datasheet indexed
                by datasheetIndex is passed. See mongomanager.component for
                more info. Defaults to None.

        Returns:
            List[dict] | pandas.DataFrame: The collected results.
        """        
        
        if chipTypes is None:
            chipTypes = ['chips']
        
        cmpLabels = self._obj._selectLabels(chipTypes, chipGroupsDict)

        allResults = []

        for label in cmpLabels:
            cmp = self._obj._allComponentsDict[label]

            scoopedResults = cmp.Datasheet.retrieveData(
                        resultNames,
                        requiredTags,
                        tagsToExclude,
                        locations = locations,
                        returnDataFrame = returnDataFrame,
                        datasheetIndex = datasheetIndex,
                        verbose = False
                    )
            
            if scoopedResults is None:
                continue
            
            # Prepending chip-identifying data

            if returnDataFrame:
                scoopedResults.insert(0, "label", len(scoopedResults)*[label])
                scoopedResults.insert(0, "componentID", len(scoopedResults)*[cmp.ID])
                scoopedResults.insert(0, "componentName", len(scoopedResults)*[cmp.name])
                scoopedResults.insert(0, "wafer", len(scoopedResults)*[self._obj.wafer.name])
            else:
                additionalInfo = {
                        'wafer': self._obj.wafer.name,
                        'componentName': cmp.name,
                        'componentID': cmp.ID,
                        'label': label,
                    }
                scoopedResults = [{**additionalInfo, **res} for res in scoopedResults]
                    
            allResults.append(scoopedResults)
            
        # Returning

        if returnDataFrame:
            return concat(allResults, ignore_index = True)

        else:
            return _joinListsOrNone(*allResults)

    def _retrieveDatadictData(self,
                    resultName:str, # A single one
                    locationGroup:str, # For that result name
                    chipTypes = None,
                    chipGroupsDict:dict = None,
                    requiredTags:list = None,
                    tagsToExclude:list = None,
                    *,
                    datasheetIndex:int = None,
                    returnValuesOnly:bool = False,
                    returnAveraged:bool = False,
                ):
        """Given a resultName and location Group, it builds up and return a
        dictionary in the form.
        
        {
            <component label 1>: {
                <location 1>: <scooped result dict>,
                <location 2>: <scooped result dict>,
                ...
            },
            <component label 2>: {
                <location 1>: <scooped result dict>,
                <location 2>: <scooped result dict>,
                ...
            },
            ...
        }
        
        Location group is used to determine the relevant locations for each
        chip. 
        
        Notice that different chips can have different locations associated to
        the same location group.

        returnValuesOnly: If True, the result dict is replaced with values only
        returnAveraged: If True, to each component label is assigned a single
            result.

        """

        cmpLabels = self._obj._selectLabels(chipTypes, chipGroupsDict)

        dataDict = {label: None for label in cmpLabels}

        for label in cmpLabels:
            
            chip = self._obj._allComponentsDict[label]
            bp = self._obj._allComponentBPdict[label]

            # Retrieving locations
            locations = bp.Locations.retrieveGroupElements(locationGroup)
            
            if locationGroup is None:
                log.warning(f'Could not retrieve locations for chip "{chip.name}".')
                continue

            dataDict[label] = {loc: None for loc in locations}

            # Scooped results for the given chip
            # List of dictionaries
            scoopedDicts = chip.Datasheet.retrieveData(
                [resultName],
                requiredTags,
                tagsToExclude,
                locations,
                returnDataFrame = False,
                datasheetIndex = datasheetIndex,
                verbose = False)

            if scoopedDicts is None: continue

            for scooped in scoopedDicts:

                loc = scooped.get('location')
                log.debug(f'Location: {loc}')

                if returnValuesOnly:
                    data = scooped.get('resultData')
                    if data is None:
                        value = None
                    else:
                        value = data.get('value')

                    log.debug(f'Value: {value}')

                    dataDict[label][loc] = value
                else:
                    dataDict[label][loc] = scooped

        if returnAveraged:
            dataDict = averageSubchipScaleDataDict(dataDict)
        
        return dataDict

    def retrieveAveragedData(self,
            resultName:str,
            locationGroup:str,
            chipTypes:list = None,
            chipGroupsDict:dict = None,
            requiredTags:list = None,
            tagsToExclude:list = None,
            *,
            datasheetIndex:int = None,
            returnValuesOnly:bool = False):
        """This method retuns a dictionary in the form
        {
            <component label 1>: <'value': <value>, 'unit': <unit>},
            <component label 2>: <'value': <value>, 'unit': <unit>},
            <component label 3>: <'value': <value>, 'unit': <unit>},
            ...
        }

        For each component as specified by chipTypes/chipGroupsDict, this method
        first searches datasheet values for each of the locations determined by
        "locationGroup". Then, the average of these values (<value>) is
        collected in the above dictionaries.

        None values or values which are not float or ints are ignored.

        Arguments can be used to select which data is actually collected.

        Args:
            resultName (str): The result name whose values are to be averaged.
            locationGroup (str): The location group associated to the result.
            chipTypes (List[str], optional): Pass a list containing any of the
                following to select which wafer components are considered:
                "chips", "testChips", "bars", "testCells".
                Defaults to ["chips"].
            chipGroupsDict (dict, optional): If passed, it can be used to filter
                which group for each chipType is plotted. Pass it in the form
                {
                    <chipType1>: <list of groups for chipType1>,
                    <chipType2>: <list of groups for chipType2>,
                    ...
                }
                Not all chip types must be present within the dictionary. If
                they are not, all the chips for that group are considered.
            requiredTags (list[str], optional): If passed, results which lack
                the tags listed here are ignored. Defaults to None.
            tagsToExclude (list[str], optional): If passed, results that have
                tags listed here are ignored. Defaults to None.

        Keyword Args:
            datasheetIndex (int, optional): If passed, the datasheet indexed
                by datasheetIndex is passed. See mongomanager.component for
                more info. Defaults to None.
            returnValuesOnly (bool, optional): If True, only a value is returned
                for each chip instead of the nested dictionary as described
                above. Defaults to False.

        Returns:
            dict: The dictionary containing the averaged result.
        """        
       
        return self._retrieveDatadictData(
                    resultName,
                    locationGroup,
                    chipTypes,
                    chipGroupsDict,
                    requiredTags,
                    tagsToExclude,
                    datasheetIndex = datasheetIndex,
                    returnValuesOnly = returnValuesOnly,
                    returnAveraged = True
                    )

    def plotData(self,
            resultName:str,
            locationGroup:str,
            chipTypes:list = None,
            chipGroupsDict:list = None,
            requiredTags:list = None,
            tagsToExclude:list = None,
            *,
            datasheetIndex:int = None,
            colormapName:str = None,
            dataRangeMin:float = None,
            dataRangeMax:float = None,
            NoneColor = None,
            clippingHighColor = None,
            clippingLowColor = None,
            BackColor = 'White',
            colorbarLabel:str = None,
            printChipLabels:bool = False,
            chipLabelsDirection:str = None,
            title:str = None,
            dpi = None,
            ):
        """Creates a subchip-scale plot for the result "resultName" and for
        the collation chips specified by chipTypes/chipGroupsDict.

        This function first retrieves a dictionary in the form
        
        {
            <component label 1>: {
                <location 1>: <scooped result dict>,
                <location 2>: <scooped result dict>,
                ...
            },
            <component label 2>: {
                <location 1>: <scooped result dict>,
                <location 2>: <scooped result dict>,
                ...
            },
            ...
        }

        where, to each component is associated a series of locations (retrieved
        from "locationGroup"), and then to each location the corresponding
        datasheet value is assigned.

        This dictionary is then used to generate the plot.

        For more info on argumetns not-listed here, see the documentation of
        waferPlotter.plotData_subchipScale() (defined inmongoreader/plotting/
        waferPlotting.py)

        Args:
            resultName (str): The name of the result to be plotted.
            locationGroup (str): The location name associated to the results.
            chipTypes (List[str], optional): Pass a list containing any of the
                following to select which wafer components are considered:
                "chips", "testChips", "bars", "testCells".
                Defaults to ["chips"].
            

        Keyword arguments (**kwargs):
            datasheetIndex (int, optional): If passed, the datasheet indexed
                by datasheetIndex is passed. See mongomanager.component for
                more info. Defaults to None.
            chipGroupsDict (dict, optional): If passed, it can be used to filter
                which group for each chipType is plotted. Pass it in the form
                {
                    <chipType1>: <list of groups for chipType1>,
                    <chipType2>: <list of groups for chipType2>,
                    ...
                }
                Not all <chipType1> must be present within the dictionary. If
                they are not, all the chips for that group are considered.
            searchDatasheetReady (bool, optional): If True, only results that
                have the field "datasheetReady" set to True are scooped.
                If "datasheetReady" is False or missing, the result is ignored.
                Defaults to True.
            requiredStatus (str, optional): If passed, only test entries whose
                "status" field is equal to requiredStatus are considered.
                Defaults to None.
            requiredProcessStage (str, optional): If passed, only test entries
                whose "processStage" field is equal to requiredStatus are
                considered. Defaults to None.
            requiredTags (list[str], optional): If passed, results which lack
                the tags listed here are ignored. Defaults to None.
            tagsToExclude (list[str], optional): If passed, results that have
                tags listed here are ignored. Defaults to None.
        """

        plt = wplt.waferPlotter(self._obj.connection, self._obj.waferBlueprint)

        if chipTypes is None: chipTypes = ['chips']

        # Should update to retrive unit!
        dataDict = self._retrieveDatadictData(
                            resultName,
                            locationGroup,
                            chipTypes,
                            chipGroupsDict,
                            requiredTags,
                            tagsToExclude,
                            datasheetIndex = datasheetIndex,
                            returnValuesOnly=True,
                            returnAveraged=False)
        
        if title is None: title = resultName

        return plt.plotData_subchipScale(dataDict,
            chipTypes = chipTypes,
            chipGroupsDict = chipGroupsDict,
            title = title,
            NoneColor = NoneColor,
            colormapName=colormapName,
            dataRangeMin = dataRangeMin,
            dataRangeMax = dataRangeMax,
            clippingHighColor = clippingHighColor,
            clippingLowColor = clippingLowColor,
            BackColor = BackColor,
            waferName = self._obj.wafer.name,
            colorbarLabel = colorbarLabel,
            printChipLabels = printChipLabels,
            chipLabelsDirection = chipLabelsDirection,
            dpi = dpi,
            )
    


    def plotAveragedData(self,
            resultName:str,
            locationGroup:str,
            requiredTags:list = None,
            tagsToExclude:list = None,
            chipTypes:list = None,
            chipGroupsDict:dict = None,
            *,
            datasheetIndex:int = None,
            colormapName:str = None,
            dataRangeMin:float = None,
            dataRangeMax:float = None,
            NoneColor = None,
            clippingHighColor = None,
            clippingLowColor = None,
            BackColor = 'White',
            colorbarLabel:str = None,
            printChipLabels:bool = True,
            chipLabelsDirection:str = None,
            title:str = None,
            dpi = None,
            ):
        """Creates a subchip-scale plot of given results.
        
        This method first calls [waferCollation].Datasheets.retrieveAveragedData(),
        to obtain a dictionary which is then used to generate the plot. See its 
        documentation for more info on how data are collected.

        For more info on argumetns not-listed here, see the documentation of
        waferPlotter.plotData_subchipScale() (defined inmongoreader/plotting/
        waferPlotting.py)

        Args:
            resultName (str): The name of the result to be plotted.
            locationGroup (str): The location name associated to the results.
            chipType_orTypes (str | List[str]): Pass any of the following
                strings or a list of them to select which wafer components are
                considered: "chips", "testChips", "bars", "testCells".
                Defaults to "chips".
            

        Keyword arguments (**kwargs):
            chipGroupsDict (dict, optional): If passed, it can be used to filter
                which group for each chipType is plotted. Pass it in the form
                {
                    <chipType1>: <list of groups for chipType1>,
                    <chipType2>: <list of groups for chipType2>,
                    ...
                }
                Not all <chipType1> must be present within the dictionary.
            searchDatasheetReady (bool, optional): If True, only results that
                have the field "datasheetReady" set to True are scooped.
                If "datasheetReady" is False or missing, the result is ignored.
                Defaults to True.
            requiredStatus (str, optional): If passed, only test entries whose
                "status" field is equal to requiredStatus are considered.
                Defaults to None.
            requiredProcessStage (str, optional): If passed, only test entries
                whose "processStage" field is equal to requiredStatus are
                considered. Defaults to None.
            requiredTags (list[str], optional): If passed, results which lack
                the tags listed here are ignored. Defaults to None.
            tagsToExclude (list[str], optional): If passed, results that have
                tags listed here are ignored. Defaults to None.
        """

        plt = wplt.waferPlotter(self._obj.connection, self._obj.waferBlueprint)
    
        if chipTypes is None: chipTypes = ['chips']

        log.debug(f'[plotResults] chipTypes: {chipTypes}')

        dataDict = self.retrieveAveragedData(
            resultName,
            locationGroup,
            chipTypes,
            chipGroupsDict,
            requiredTags,
            tagsToExclude,
            datasheetIndex = datasheetIndex,
            returnValuesOnly = True)
        
        if title is None: title = resultName + ' (Averaged)'

        plt.plotData_chipScale(dataDict,
            chipTypes = chipTypes,
            chipGroupsDict = chipGroupsDict,
            title = title,
            NoneColor = NoneColor,
            colormapName=colormapName,
            dataRangeMin = dataRangeMin,
            dataRangeMax = dataRangeMax,
            clippingHighColor = clippingHighColor,
            clippingLowColor = clippingLowColor,
            BackColor = BackColor,
            waferName = self._obj.wafer.name,
            colorbarLabel = colorbarLabel,
            printChipLabels = printChipLabels,
            chipLabelsDirection = chipLabelsDirection,
            dpi = dpi,
            )

        return dataDict


@_attributeClassDecoratorMaker(_Datasheets)
class waferCollation(c.collation):
    """A waferCollation is a class used to collect from the database a wafer,
    its related components (chips, test chips, bars) and their blueprints.
    
    The collation has useful methods to extract and plot data from this
    set of components.

    A waferCollation has also attributes to access the components easily
    
    <waferCollation>.chips: A list of all the chips
    <waferCollation>.testChips: A list of all the test chips
    <waferCollation>.testCells: A list of all the wafer's test cells.
    <waferCollation>.bars: A list of all the wafer's bars.

    <waferCollation>.chipsDict: A dictionary to obtain chips from their key.
    <waferCollation>.testChipsDict: A dictionary to obtain test chips from their key.
    <waferCollation>.testCellsDict: A dictionary to obtain test cells from their key.
    <waferCollation>.barsDict: A dictionary to obtain bars from their key.

    Similarly to above, we have the same for blueprints:

    <waferCollation>.chipBlueprints
    <waferCollation>.testChipBlueprints
    <waferCollation>.testCellBlueprints
    <waferCollation>.barBlueprints

    <waferCollation>.chipBPdict
    <waferCollation>.testChiBPdict
    <waferCollation>.testCellBPdict
    <waferCollation>.barBPdict

    """

    def __init__(self, connection:mom.connection, waferName_orCmp_orID):
        """Initialization method of the waferCollation class.

        The main purpose of this method is to retrieve the component and
        blueprint documents for wafers, bars, chips, test chips and test cells.
        assigning them to corresponding attributes of the waferCollation
        instance.

        It also constructs dictioaries to easily access components through their
        labels.

        Args:
            connection (mongomanager.connection): The connection instance to 
                the MongoDB server.
            waferName_orCmp_orID (str | mongomanager.component | ObjectId): The 
                collation wafer.

        Raises:
            ImplementationError: When 
            TypeError: If arguments are not specified correctly.
        """        

        if not isinstance(connection, mom.connection):
            raise TypeError('"connection" must be a mongomanager.connection object.')

        self.connection = connection

        with opened(connection):

            # Collecting wafer

            if isinstance(waferName_orCmp_orID, mom.wafer):
                self.wafer = waferName_orCmp_orID
            else:
                self.wafer = self._collectWafer(waferName_orCmp_orID)

            # Collecting wafer blueprint
            self.waferBlueprint = self._collectWaferBlueprint(self.wafer)
            
        
            # Collecting chip blueprints
            self.chipBlueprints, self.chipBPdict = \
                 self._collectChipBlueprints(self.waferBlueprint)

            # Collecting test chip blueprints
            self.testChipBlueprints, self.testChipBPdict = \
                 self._collectTestChipBlueprints(self.waferBlueprint)

            # Collecting bar blueprints
            self.barBlueprints, self.barBPdict = \
                 self._collectBarBlueprints(self.waferBlueprint)

            # Collecting test cell blueprints
            self.testCellBlueprints, self.testCellBPdict = \
                 self._collectTestCellBlueprints(self.waferBlueprint)

            self._allComponentBPdict = _joinDictsOrNone(
                self.chipBPdict,
                self.testChipBPdict,
                self.barBPdict,
                self.testCellBPdict
            )

            # Dictionary containing all the chipType -> allowed groups relations
            self._allowedGroupsDict = {
                'chips': self.waferBlueprint.ChipBlueprints.retrieveGroupNames(),
                'testChips': self.waferBlueprint.TestChipBlueprints.retrieveGroupNames(),
                'testCells': self.waferBlueprint.TestCellBlueprints.retrieveGroupNames(),
                'bars': self.waferBlueprint.BarBlueprints.retrieveGroupNames(),
            }


            # Collecting chips, testChips and bars
            self.chips, self.chipsDict, \
            self.testChips, self.testChipsDict, \
            self.bars, self.barsDict = \
                self._collectChipsalike(self.wafer,
                            self.chipBlueprints, self.chipBPdict,
                            self.testChipBlueprints, self.testChipBPdict,
                            self.barBlueprints, self.barBPdict,
                            )

            self.allChips = _joinListsOrNone(self.chips, self.testChips)
            self.allChipsDict = _joinDictsOrNone(self.chipsDict, self.testChipsDict)

            # Collecting testCells
            self.testCells, self.testCellsDict = \
                self._collectTestCells(self.wafer, self. testCellBlueprints, self.testCellBPdict)
            
            self._allComponentsDict = _joinDictsOrNone(self.allChipsDict, self.barsDict, self.testCellsDict)
        
            # wafer plotter

            self.wplt = wplt.waferPlotter(self.connection, self.waferBlueprint)


    # --- collect methods ---

    def _collectWafer(self, waferName_orID):
        """Queries the database for the specified wafer and returns it.

        Args:
            waferName_orID (str | ObjectId): The wafer name or its ID.

        Raises:
            DocumentNotFound: If the wafer is not found.
            TypeError: If arguments are not specified correctly.

        Returns:
            wafer: The collected wafer.
        """


        if isinstance(waferName_orID, str):

            wafer = mom.wafer.queryOneByName(self.connection, waferName_orID,
                            verbose = False)
        
            if wafer is None:
                raise DocumentNotFound(f'Could not find a wafer named "{waferName_orID}".')

        elif isID(waferName_orID):
           
            with mom.logMode(mom.log, 'WARNING'):
                wafer = mom.wafer.importDocument(self.connection, waferName_orID, verbose = False)

            if wafer is None:
                raise DocumentNotFound(f'Could not import a wafer from ID "{waferName_orID}".')

        else:
            raise TypeError(f'"waferName_orID" must be a string or an ID.')

        log.info(f'Collected wafer "{wafer.name}"')
        return wafer
    

    def _collectWaferBlueprint(self, wafer):
        """Returns the wafer blueprint of the wafer.
        
        Raises:
            DocumentNotFound: If the blueprint is not found.
            
        Returns:
            waferBlueprint: The retrieved wafer blueprint document.
        """

        wbp = wafer.retrieveBlueprint(self.connection, verbose = False)
    
        if wbp is None:
            raise DocumentNotFound('Could not retrieve the wafer blueprint.')

        log.info(f'Collected wafer blueprint "{wbp.name}".')
        return wbp


    def _collectChipBPalikes(self, waferBlueprint, BPtype:str):

        assert isinstance(BPtype , str), '"BPtype" must be a string.'

        assert BPtype in ["chipBlueprints", "testChipBlueprints", "barBlueprints", "testCellBlueprints"], \
            '"BPtype" must be among "chipBlueprints", "testChipBlueprints", "barBlueprints" and "testCellBlueprints"'
        
        if BPtype == "chipBlueprints":
            attributeClass = waferBlueprint.ChipBlueprints

        elif BPtype == "testChipBlueprints":
            attributeClass = waferBlueprint.TestChipBlueprints

        elif BPtype == "barBlueprints":
            attributeClass = waferBlueprint.BarBlueprints

        elif BPtype == "testCellBlueprints":
            attributeClass = waferBlueprint.TestCellBlueprints

        else:
            raise ValueError(f'"BPtype" ("{BPtype}") is not among "chipBlueprints", "testChipBlueprints", "barBlueprints" and "testCellBlueprints".')

        # {ID: {'document': <blueprint>, 'labels': <list of labels>}, ...}
        BPsDict = attributeClass.retrieveElements(self.connection, grouped = True, verbose = False)

        if BPsDict is None:
            log.warning(f'Could not retrieve any {BPtype}.')
            return None, None

        # I want a reversed dictionary:
        # {<label>: <blueprint>, ...}

        bpDict_labelsBPs = {}
        for ID, dict in BPsDict.items():
            for label in dict.get('labels', []):
                bpDict_labelsBPs[label] = dict.get('document')
        
        bps = [dic['document'] for dic in BPsDict.values()]

        if bps == []: bps = None
        if bpDict_labelsBPs == {}: bpDict_labelsBPs = None
        
        amount = len(bps) if bps is not None else 0  
        log.info(f'Collected {amount} {BPtype}.')

        return bps, bpDict_labelsBPs


    def _collectChipBlueprints(self, waferBlueprint):
        """Returns the chip blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'chipBlueprints')

    def _collectTestChipBlueprints(self, waferBlueprint):
        """Returns the test chip blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'testChipBlueprints')

    def _collectBarBlueprints(self, waferBlueprint):
        """Returns the test bar blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'barBlueprints')

    def _collectTestCellBlueprints(self, waferBlueprint):
        """Returns the test cell blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'testCellBlueprints')
        

    def _collectChipsalike_commonFunction(self, what, allChips, bps, bpDict):

        expectedAmount = len(bpDict) if bpDict is not None else 0

        if bps is None:
            log.spare(f'No blueprints for {what} blueprints. No {what}s collected')
            return None, None

        bpIDs = [bp.ID for bp in bps]

        log.debug(f'[_collectChipsalike_commonFunction] ({what}): bpIDs: {bpIDs}')
        log.debug(f'[_collectChipsalike_commonFunction] ({what}): bpDict: {bpDict}')

        # Collecting chips
        chips = [chip for chip in allChips if chip.blueprintID in bpIDs]
        log.info(f'Collected {len(chips)} {what}s.')
        if len(chips) != expectedAmount:
            log.warning(f'Collected {len(chips)} {what}s but {expectedAmount} were expected.')

        # Creating dictionary
        chipsDict = {}
        for chip in chips:
            label = chip.getField('_waferLabel')
            if label is not None:
                chipsDict[label] = chip
            else:
                log.warning(f'Could not retrieve wafer label for {what} "{chip.name}". Not included in {what}sDict.')


        # Checking label exists in the wafer
        for label in chipsDict:
            if not label in bpDict:
                log.error(f'Label "{label}" does not exist in the wafer blueprint dictionary!')
        
        if chips == []: chips = None
        if chipsDict == {}: chipsDict = None

        return chips, chipsDict


    def _collectChipsalike(self, wafer,
                     chipBPs, chipBPsDict,
                     testBPs, testChipBPsDict,
                     barBPs, barBPsDict,
                     ):

        allChipsalike = wafer.ChildrenComponents.retrieveElements(self.connection, verbose = False)
        
        if allChipsalike is None:
            log.warning('I retrieved no children components from the wafer.')
            return None, None, None, None, None, None


        # Chips
        chips, chipsDict = \
            self._collectChipsalike_commonFunction('chip', allChipsalike, chipBPs, chipBPsDict)
        
        # Test chips
        testChips, testChipsDict = \
            self._collectChipsalike_commonFunction('test chip', allChipsalike, testBPs, testChipBPsDict)
        
        # Bars
        bars, barsDict = \
            self._collectChipsalike_commonFunction('bar', allChipsalike, barBPs, barBPsDict)

        return chips, chipsDict, testChips, testChipsDict, bars, barsDict

    def _collectTestCells(self, wafer, cellBPs, cellBPsDict):

        testCells = wafer.TestCells.retrieveElements(self.connection, verbose = False)
        
        # testCells
        testCells, testCellsDict = \
            self._collectChipsalike_commonFunction('test cell', testCells, cellBPs, cellBPsDict)
        
        return testCells, testCellsDict

    # ---------------------------------------------------
    # Internal utility functions

    _allowedChipTypes = ["chips", "testChips", "testCells", "bars"]

    def _componentsFromType(self, chipTypes:list):

        if not isinstance(chipTypes, list):
            raise TypeError('"chipTypes" must be a list of strings.')
        if not all([isinstance(el, str) for el in chipTypes]):
            raise TypeError('"chipTypes" must be a list of strings.')

        if not all([el in self._allowedChipTypes for el in chipTypes]):
            raise ValueError('"chipTypes" must contain strings among '\
                             +', '.join(f'"{s}"' for s in self._allowedChipTypes))
        
        cmpsToReturn = []

        for chipType in chipTypes:
            
            if chipType == "chips":
                cmpsToReturn += self.chips
            elif chipType == 'testChips':
                cmpsToReturn += self.testChips
            elif chipType == "testCells":
                cmpsToReturn += self.testCells
            elif chipType == "bars":
                cmpsToReturn += self.bars

        return cmpsToReturn
        
    def _waferBlueprintAttributeClassFromType(self, chipType:str):

        if not isinstance(chipType, str):
            raise TypeError(f'"chipTypes" must be a string.')
        
        if not chipType in self._allowedChipTypes:
            raise ValueError(f'"chipType" must be among {self._allowedChipTypes}')

        if chipType == "chips":
            return self.waferBlueprint.ChipBlueprints
        elif chipType == "testChips":
            return self.waferBlueprint.TestChipBlueprints
        elif chipType == "testCells":
            return self.waferBlueprint.TestCellBlueprints
        elif chipType == "bars":
            return self.waferBlueprint.BarBlueprints
        


    # ---------------------------------------------------


    def refresh(self):
        """Refreshes from the database all the components and blueprints of
        the collation.
        
        It calls mongoRefresh() on all of them."""
        
        with opened(self.connection):
            
            self.wafer.mongoRefresh(self.connection)
            log.spare(f'Refreshed wafer "{self.wafer.name}".')

            self.waferBlueprint.mongoRefresh(self.connection)
            log.spare(f'Refreshed waferBlueprint "{self.waferBlueprint.name}".')

            if self.chips is not None:
                for chip in self.chips: chip.mongoRefresh(self.connection)
                log.spare(f'Refreshed chips.')

            if self.testChips is not None:
                for chip in self.testChips: chip.mongoRefresh(self.connection)
                log.spare(f'Refreshed test chips.')

            if self.testChips is not None:
                for bar in self.bars: bar.mongoRefresh(self.connection)
                log.spare(f'Refreshed bars.')

            if self.testChips is not None:
                for cell in self.testCells: cell.mongoRefresh(self.connection)
                log.spare(f'Refreshed test cells.')

            if self.testChips is not None:
                for bp in self.chipBlueprints: bp.mongoRefresh(self.connection)
                log.spare(f'Refreshed chip blueprints.')

            if self.testChips is not None:
                for bp in self.testChipBlueprints: bp.mongoRefresh(self.connection)
                log.spare(f'Refreshed test chip blueprints.')

            if self.testChips is not None:
                for bp in self.barBlueprints: bp.mongoRefresh(self.connection)
                log.spare(f'Refreshed bar blueprints.')

            if self.testChips is not None:
                for bp in self.testCellBlueprints: bp.mongoRefresh(self.connection)
                log.spare(f'Refreshed test chip blueprints.')


    # ---------------------------------------------------
    # Data retrieval methods

    def _selectLabels(self, chipTypes:list = None, chipGroupsDict:dict = None):
        """Given the combination of chipTypes and chipGroupsDict, it returns
        the list of labels to which they correspond

        Args:
            chipTypes (list[str] | None): The macro-groups ("chips",
                "testChips", "bars", "testCells")
            chipGroupsDict (dict | None): The sub-groups for each of the
                macro groups.
        """

        # Type checks
        if chipTypes is not None:

            for el in chipTypes:
                if not isinstance(el, str):
                    raise TypeError(f'"chipTypes" must None or a list of strings among {self._allowedChipTypes}.')
                if not all([el in self._allowedChipTypes for el in chipTypes]):
                    raise ValueError(f'"chipTypes" must None or a list of strings among {self._allowedChipTypes}.')

        if chipGroupsDict is not None:

            if not isinstance(chipGroupsDict, dict):
                raise TypeError(f'"chipGroupsDict" must be a dictionary.')

            if not all([key in chipTypes for key in chipGroupsDict]):
                raise ValueError(f'The keys of "chipGroupsDict" must be among "chipTypes" ({chipTypes}).')

            for ctype, groups in chipGroupsDict.items():
                
                if groups is not None:
                    if not isinstance(groups, list):
                        raise TypeError(f'Values of "groupsDict" must be None or a list of strings among self.allowedGroupsDict.')
                    
                    if not all([g in self._allowedGroupsDict[ctype] for g in groups]):
                        raise ValueError(f'Values of "groupsDict" must be None or a list of strings among self.allowedGroupsDict.')

        chipSerials = []

        if chipTypes is None:
            chipTypes = self._allowedChipTypes
        
        if chipGroupsDict is None:
            chipGroupsDict = {chipType: None for chipType in chipTypes}

        log.debug(f'[_selectLabels] chipTypes: {chipTypes}')
        log.debug(f'[_selectLabels] chipGroupsDict: {chipGroupsDict}')

        for chipType, chipGroups in chipGroupsDict.items():

            attributeClass = self._waferBlueprintAttributeClassFromType(chipType)

            if chipGroups is None:
                newLabels = attributeClass.retrieveLabels()
                log.debug(f'[_selectLabels] Chip groups is None: newLabels: {newLabels}')
                if newLabels is None: continue

            else:
                newLabels = []
                for group in chipGroups:
                    newLabels = attributeClass.retrieveGroupLabels(group)
                    if newLabels is None: continue
                    chipSerials += newLabels
                    log.debug(f'[_selectLabels] newLabels for group "{group}": {newLabels}')


            chipSerials += newLabels

        if chipSerials == []:
            return None
        
        return chipSerials


    def retrieveAllTestResultNames(self):
        """Returns the list of all the test result names found in the test
        history of all the chips of the collation.
        
        Returns None if no result name is found."""

        allNames = []
        for chip in self.chips:
            testNames = chip.retrieveTestResultNames()
            
            if testNames is None:
                continue
            
            for name in testNames:
                if name not in allNames:
                    allNames.append(name)
        
        if allNames == []: return None

        return allNames


    def _scoopTestResults(self,
            chipTypes:list = None,
            chipGroupsDict:dict = None,
            resultNames:list = None,
            locationGroups:list = None,
            searchDatasheetData:bool = False,
            requiredStati:list = None,
            requiredProcessStages:list = None,
            requiredTags:list = None,
            tagsToExclude:list = None,
            earliestExecutionDate = None,
            latestExecutionDate = None,
            requiredTestReportID = None,
            *,
            returnType:str = 'dictionary',
            verbose:bool = True,
    ):
        """Internal method to scoop results from the test history of wafer
        compoents.
        
        Results are formatted in different ways depending on the value of
        "returnType". For other methods see
        mongomanager
            └ goggleFunctions
                └ componentGoggleFunctions
                    └ scoopComponentResults()
        """

        log.debug(f'[_scoopTestResults] chipTypes: {chipTypes}')
        log.debug(f'[_scoopTestResults] chipGroupsDict: {chipGroupsDict}')
        log.debug(f'[_scoopTestResults] resultNames: {resultNames}')
        log.debug(f'[_scoopTestResults] locationGroups: {locationGroups}')

        returnTypes = ['dictionary', 'DataFrame', 'testResult', 'dataDictionary']
        returnTypesStr = ', '.join([f'"{s}"' for s in returnTypes])

        if not isinstance(returnType, str):
            raise TypeError(f'"returnType" must be a string among {returnTypesStr}')
        if not returnType in returnTypes:
            raise ValueError(f'"returnType" must be a string among {returnTypesStr}')

        if returnType == 'dictionary':
            scoopReturnType = 'dictionary'
        elif returnType == 'DataFrame':
            scoopReturnType = 'DataFrameDictionary'
        elif returnType == 'testResult':
            scoopReturnType = 'testResult'
        elif returnType == 'dataDictionary':
            scoopReturnType = 'DataFrameDictionary'

        # Determining chip serials to be considered
        chipSerials = self._selectLabels(chipTypes, chipGroupsDict)

        if chipSerials is None:
            if verbose: mom.log.warning(f'No chip serials have been determined!')
            return None
        
        # Location groups to location lists
        locationDict = {}

        if locationGroups is None:
            locationDict = {serial: None for serial in chipSerials}
        else:
            for serial in chipSerials:
                locationDict[serial] = []
                bp = self._allComponentBPdict[serial]

                for locGroup in locationGroups:
                    locNames = bp.Locations.retrieveGroupElements(locGroup, verbose = False)
                    if locNames is not None:
                        locationDict[serial] += locNames

        allScooped = []
        for serial in chipSerials:

            cmp = self._allComponentsDict[serial]

            scooped = cmpGoggles.scoopComponentResults(
                            self._allComponentsDict[serial],
                            resultNames,
                            locationDict[serial], # All location names
                            searchDatasheetData,
                            requiredStati,
                            requiredProcessStages,
                            requiredTags,
                            tagsToExclude,
                            earliestExecutionDate,
                            latestExecutionDate,
                            requiredTestReportID,
                            returnType = scoopReturnType)
            
            if scooped is None: continue

            if returnType == 'dataDictionary':
                for s in scooped:
                    minimal = {
                        'label': serial,
                        'value': s.get('resultValue'),
                        'location': s.get('location'),
                    }
                    allScooped.append(minimal)
                continue
            
            additional = {'wafer': self.wafer.name,
                          'componentName': cmp.name,
                          'componentID': cmp.ID,
                          'label': serial}
            
            for s in scooped:
                complete = {**additional, **s}
                allScooped.append(complete)
            # Adding wafer-scale information

        if returnType == 'dictionary':
            return allScooped
        elif returnType == 'DataFrame':
            return DataFrame([s for s in allScooped])
        elif returnType == 'testResult':
            return allScooped
        elif returnType == 'dataDictionary':
            return allScooped
        
        else:
            raise Exception('Nothing to return. Check returnType!')

    def retrieveTestResults(self,
        resultName_orNames = None,
        locationGroup_orGroups = None,
        chipType_orTypes = None,
        chipGroupsDict:dict = None,
        searchDatasheetData:bool = False,
        requiredStatus_orList = None,
        requiredProcessStage_orList = None,
        requiredTags:list = None,
        tagsToExclude:list = None,
        earliestExecutionDate = None,
        latestExecutionDate = None,
        requiredTestReportID = None,
        *,
        returnDataFrame:bool = False,
        verbose:bool = True):
        """Returns results collected from the test history of components of the
        wafer collation.

        Args:
            resultName_orNames (str | List[str], optional): If passed, result(s)
                whose name is not listed here are ignored.
            locationGroup_orGroups (str | List[str], optional): If passed,
                results whose location is not among correponding to these
                location groups are ignored.
            chipType_orTypes (str | List[str]): Pass any of the following
                strings or a list of them to select which wafer components are
                considered: "chips", "testChips", "bars", "testCells".
                Defaults to "chips".
            chipGroupsDict (dict, optional): If passed, it can be used to filter
                which group for each chipType is plotted. Pass it in the form
                {
                    <chipType1>: <list of groups for chipType1>,
                    <chipType2>: <list of groups for chipType2>,
                    ...
                }
                Not all chip type must be present within the dictionary. If
                they are not, all the chips for that group are considered.
            searchDatasheetData (bool, optional): If True, only results for
                which "datasheetReady" = True are collected. Defaults to False.
            requiredStatus_orList (str | List[str]): If passed, only results
                for which the chip status was among these are collected.
            requiredProcessStage_orList (str | List[str]): If passed, only
                results for which the chip processStage was among these are
                collected.
            requiredTags (list, optional): If passed, results tags must contain
                those listed here to be collected.
            tagsToExclude (list, optional): If passed, results whose tags are
                among these are not collected. Defaults to None.
            earliestExecutionDate (aware datetime object, optional): If passed,
                only results generated after this date are collected.
            latestExecutionDate (aware datetime object, optional): If passed,
                only results generated before this date are collected.
            requiredTestReportID (ID, optional): If passed, only results whose
                testReportID is equal to this are collected.
        
                
        Keyword Args:
            returnDataFrame (bool, optional): If True, data are returned as
                a pandas DataFrame. Defaults to False
            verbose (bool, optional): If False, warning messages are suppressed.
                Defaults to True.
        
        Returns:
            List[dict] | DataFrame | None
        """

        def normalizeArgument(arg):
            if arg is None: return None

            if isinstance(arg, list): return arg

            return [arg]

        chipTypes = normalizeArgument(chipType_orTypes)
        resultNames = normalizeArgument(resultName_orNames)
        locationGroups = normalizeArgument(locationGroup_orGroups)
        requiredStati = normalizeArgument(requiredStatus_orList)
        requiredProcessStages = normalizeArgument(requiredProcessStage_orList)
        
        if chipTypes is None: chipTypes = ['chips']

        if returnDataFrame:
            returnType = 'DataFrame'
        else:
            returnType = 'dictionary'

        return self._scoopTestResults(chipTypes,
                                      chipGroupsDict,
                                      resultNames,
                                      locationGroups,
                                      searchDatasheetData,
                                      requiredStati,
                                      requiredProcessStages,
                                      requiredTags,
                                      tagsToExclude,
                                      earliestExecutionDate,
                                      latestExecutionDate,
                                      requiredTestReportID,
                                      returnType = returnType,
                                      verbose = verbose)


    def retrieveAveragedTestData(self,
        resultName_orNames,
        chipType_orTypes,
        chipGroupsDict:dict = None,
        locationGroup_orGroups = None,
        searchDatasheetData:bool = False,
        requiredStatus_orList = None,
        requiredProcessStage_orList = None,
        requiredTags:list = None,
        tagsToExclude:list = None) -> dict:
        """Works like .retrieveTestData, but values are averaged to the chip-scale
            level."""
        raise NotImplementedError()

        dataDict_subchip = self.retrieveTestData(resultName_orNames,
            chipType_orTypes,
            chipGroupsDict,
            locationGroup_orGroups,
            searchDatasheetData,
            requiredStatus_orList,
            requiredProcessStage_orList,
            requiredTags,
            tagsToExclude)
        
        dataDict = averageSubchipScaleDataDict(dataDict_subchip)

        return dataDict
        

    def plotField(self,
                  field_or_chain,
                  chipTypes:list = None,
                  dataType:str = None,
                  colorDict:dict = None,
                  title:str = None):
        """Plots a given field for all the chips specified by chipTypes.

        Args:
            field_or_chain (str | list[str|int]): The chain of keys/indexes that
                identifies the dictionary element to plot
            chipTypes (list[str], optional): The types of chips considered for
                the plots. Elements must be among "chips", "testChips", "bars"
                and "testCells". Defaults to ["chips"].
            dataType (str, optional): If passed, the plot will assume that data
                is either of that type or None, and will mark different data
                as not valid. Must be among "float", "string" and "bool".
                Defaults to None, in which case the function attempts to
                determine automatically the type.
            colorDict (dict, optional): The custom color dictionary used for
                string-type data. Defaults to None.
            title (str, optional): The title of the plot. Defaults to None.
        """        

        if chipTypes is None:
            chipTypes = ['chips']

        cmps = self._componentsFromType(chipTypes)

        dataDict = {cmp['_waferLabel']: cmp.getField(field_or_chain, verbose = False)
            for cmp in cmps}
        
        return self.wplt.plotData_chipScale(
                    dataDict,
                    dataType = dataType,
                    chipTypes = chipTypes,
                    title = title,
                    colormapName='rainbow',
                    waferName = self.wafer.name,
                    printChipLabels = True,
                    colorDict=colorDict,
            )

    def plotStatus(self,
                chipTypes:list = None,
                colorDict:dict = None,
                title:str = 'Status'):
        """Plots the status of all the chips specified by chipTypes.

        Args:
            chipTypes (list[str], optional): The types of chips considered for
                the plots. Elements must be among "chips", "testChips", "bars"
                and "testCells". Defaults to ["chips"].
            colorDict (dict, optional): The custom color dictionary used for
                string-type data. Defaults to None.
            title (str, optional): The title of the plot. Defaults to None.
        """

        return self.plotField(
                'status',
                chipTypes = chipTypes,
                dataType = 'string',
                colorDict = colorDict,
                title = title)
    
    def plotProcessStage(self,
                chipTypes:list = None,
                colorDict:dict = None,
                title:str = 'Process Stage'):
        """Plots the process stage of all the chips specified by chipTypes.

        Args:
            chipTypes (list[str], optional): The types of chips considered for
                the plots. Elements must be among "chips", "testChips", "bars"
                and "testCells". Defaults to ["chips"].
            colorDict (dict, optional): The custom color dictionary used for
                string-type data. Defaults to None.
            title (str, optional): The title of the plot. Defaults to None.
        """

        return self.plotField(
                'processStage',
                chipTypes = chipTypes,
                dataType = 'string',
                colorDict = colorDict,
                title = title)
    
    def _instanciateDataDict(self, chipLabels:list, locationGroup:str):
        """Generates an empty dataDictionary (sub-chip scale)."""
        
        if not isListOfStrings(chipLabels):
            raise TypeError('"chipLabels" must be a list of strings.')
        if not isinstance(locationGroup, str):
            raise TypeError('"locationGroup" must be a string.')

        dataDict = {label: None for label in chipLabels}

        for label in chipLabels:

            bp = self._allComponentBPdict[label]

            locNames = bp.Locations.retrieveGroupElements(locationGroup, verbose = False)
            if locNames is not None:
                dataDict[label] = {loc: None for loc in locNames}

        return dataDict


    def plotTestResults(self,
        resultName:str = None,
        locationGroup:str = None,
        chipType_orTypes = None,
        chipGroupsDict:dict = None,
        searchDatasheetData:bool = False,
        requiredStatus_orList = None,
        requiredProcessStage_orList = None,
        requiredTags:list = None,
        tagsToExclude:list = None,
        earliestExecutionDate = None,
        latestExecutionDate = None,
        requiredTestReportID = None,
        *,
        colormapName:str = None,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        NoneColor = None,
        clippingHighColor = None,
        clippingLowColor = None,
        BackColor = 'White',
        colorbarLabel:str = None,
        printChipLabels:bool = False,
        chipLabelsDirection:str = None,
        title:str = None,
        dpi = None,

        verbose:bool = True,
        ):
        """Creates a subchip-scale plot of results present in the test history
        of the collation components.

        The results are collected using [waferCollation].retrieveTestResults().
        See its documentation for non-keyword arguments of this method.
        
        The plot is generated using waferPlotter.plotData_subchipScale()
        (defined in mongoreader/plotting/waferPlotting.py). See its
        documentation for the keyword arguments of this method.
        """

        def normalizeArgument(arg):
            if arg is None: return None

            if isinstance(arg, list): return arg

            return [arg]

        chipTypes = normalizeArgument(chipType_orTypes)
        resultNames = normalizeArgument(resultName)
        locationGroups = normalizeArgument(locationGroup)
        requiredStati = normalizeArgument(requiredStatus_orList)
        requiredProcessStages = normalizeArgument(requiredProcessStage_orList)

        if chipTypes is None: chipTypes = ['chips']

        # Preparations
        chipSerials = self._selectLabels(chipTypes, chipGroupsDict)

        if chipSerials is None:
            if verbose: log.warning('No component labels identified. Nothing to plot.')
            return None
        
        dataDict = self._instanciateDataDict(chipSerials, locationGroup)

        log.debug(f'[plotTestResults] chipTypes: {chipTypes}')
        log.debug(f'[plotTestResults] chipGroupsDict: {chipGroupsDict}')
        log.debug(f'[plotTestResults] resultNames: {resultNames}')
        log.debug(f'[plotTestResults] locationGroups: {locationGroups}')

        scooped = self._scoopTestResults(
                                chipTypes,
                                chipGroupsDict,
                                resultNames,
                                locationGroups,
                                searchDatasheetData,
                                requiredStati,
                                requiredProcessStages,
                                requiredTags,
                                tagsToExclude,
                                earliestExecutionDate,
                                latestExecutionDate,
                                requiredTestReportID,
                                returnType = 'dataDictionary',
                                verbose = True,
            
        ) # List of dicts

        if scooped is None:
            if verbose: log.warning('No results were retrieved. Nothing to plot.')
        
        # Populating dataDict from scooped results
        if scooped is not None:
            for s in scooped:

                log.debug(f'Scooped: {s}')

                label = s['label']
                loc = s['location']
                value = s['value']

                if label in dataDict:
                    if loc in dataDict[label]:
                        
                        if dataDict[label][loc] is not None: # A value is already present
                            if verbose: log.warning(f'Multiple values scooped for "{label}"-"{loc}".')

                        log.debug(f'Adding value to "{label}"-"{loc}": {value}')
                        dataDict[label][loc] = value
        
        # Plotting

        plt = wplt.waferPlotter(self.connection, self.waferBlueprint)

        if title is None: title = resultName

        return plt.plotData_subchipScale(dataDict,
            chipTypes = chipTypes,
            chipGroupsDict = chipGroupsDict,
            title = title,
            NoneColor = NoneColor,
            colormapName=colormapName,
            dataRangeMin = dataRangeMin,
            dataRangeMax = dataRangeMax,
            clippingHighColor = clippingHighColor,
            clippingLowColor = clippingLowColor,
            BackColor = BackColor,
            waferName = self.wafer.name,
            colorbarLabel = colorbarLabel,
            printChipLabels = printChipLabels,
            chipLabelsDirection = chipLabelsDirection,
            dpi = dpi,
            )


    def plotAveragedTestResults(self,
        resultName:str,
        locationGroup:str,
        chipType_orTypes = None,
        *,
        chipGroupsDict:dict = None,
        colormapName:str = None,
        dataRangeMin:float = None,
        dataRangeMax:float = None,
        NoneColor = None,
        clippingHighColor = None,
        clippingLowColor = None,
        BackColor = 'White',
        colorbarLabel:str = None,
        printChipLabels:bool = True,
        chipLabelsDirection:str = None,
        title:str = None,
        dpi = None,
        **kwargs):
        """Works like plotResults, but data is first averaged to a chip-scale
        level."""
        raise NotImplementedError()

        plt = wplt.waferPlotter(self.connection, self.waferBlueprint)
    
        if chipType_orTypes is None:
            chipTypes = ['chips']
        elif not isinstance(chipType_orTypes, list):
            chipTypes = [chipType_orTypes]
        else:
            chipTypes = chipType_orTypes


        dataDict = self.retrieveAveragedTestData(
            resultName,
            chipType_orTypes,
            chipGroupsDict,
            locationGroup,
            **kwargs
        )

        if title is None: title = resultName + ' (Averaged)'

        plt.plotData_chipScale(dataDict,
            title = title,
            chipTypes = chipTypes,
            chipGroupsDict = chipGroupsDict,
            NoneColor = NoneColor,
            colormapName=colormapName,
            dataRangeMin = dataRangeMin,
            dataRangeMax = dataRangeMax,
            clippingHighColor = clippingHighColor,
            clippingLowColor = clippingLowColor,
            BackColor = BackColor,
            waferName = self.wafer.name,
            colorbarLabel = colorbarLabel,
            printChipLabels = printChipLabels,
            chipLabelsDirection = chipLabelsDirection,
            dpi = dpi,
            )

        return dataDict




    @staticmethod
    def chipSummaryDict(chip):

        dic = {
                'name': chip.name,
                'ID': chip.ID,
                'status': chip.status,
                'tags': chip.getTags(),
                'statusDateOfChange': chip.getField(['statusLog', -1, 'dateOfChange'], verbose = False), # Could be wrong.
                'hasNotes': chip.hasNotes(),
                'hasWarnings': chip.hasWarnings(),
                'hasLog': chip.hasLog(),
                'hasTags': chip.hasTags(),
                'hasTestHistory': chip.hasTestHistory(),
                'hasProcessHistory': chip.hasProcessHistory(),
                'hasFiles': chip.hasFiles(),
                'hasImages': chip.hasImages(),
                'hasAttachments': chip.hasAttachments(),
            }

        return dic

    @staticmethod
    def barSummaryDict(bar):

        dic = {
                'name': bar.name,
                'ID': bar.ID,
                'status': bar.status,
                'tags': bar.getTags(),
                'statusDateOfChange': bar.getField(['statusLog', -1, 'dateOfChange'], verbose = False), # Could be wrong.
                'hasNotes': bar.hasNotes(),
                'hasWarnings': bar.hasWarnings(),
                'hasLog': bar.hasLog(),
                'hasTags': bar.hasTags(),
                'hasTestHistory': bar.hasTestHistory(),
                'hasProcessHistory': bar.hasProcessHistory(),
                'hasFiles': bar.hasFiles(),
                'hasImages': bar.hasImages(),
                'hasAttachments': bar.hasAttachments(),
            }

        return dic

    @staticmethod
    def waferSummaryDict(wafer):

        dic = {
                'name': wafer.name,
                'ID': wafer.ID,
                'status': wafer.status,
                'tags': wafer.getTags(),
                'statusDateOfChange': wafer.getField(['statusLog', -1, 'dateOfChange'], verbose = False), # Could be wrong.
                'hasNotes': wafer.hasNotes(),
                'hasWarnings': wafer.hasWarnings(),
                'hasLog': wafer.hasLog(),
                'hasTags': wafer.hasTags(),
                'hasTestHistory': wafer.hasTestHistory(),
                'hasProcessHistory': wafer.hasProcessHistory(),
                'hasFiles': wafer.hasFiles(),
                'hasImages': wafer.hasImages(),
                'hasAttachments': wafer.hasAttachments(),
            }

        return dic
    
    def chipsSummaryDict(self):

        if self.chips is None:
            return {}
        else:
            return {chip.name: self.chipSummaryDict(chip) for chip in self.chips}

    def barsSummaryDict(self):

        if self.bars is None:
            return {}
        else:
            return {bar.name: self.chipSummaryDict(bar) for bar in self.bars}

    def summaryDict(self):

        dic = {
            'wafer': self.waferSummaryDict(self.wafer),
            'chips': self.chipsSummaryDict(),
            'bars': self.barsSummaryDict(),
        }

        return dic


    # ------------------------------------------
    # Print methods

    @staticmethod
    def _printWaferInfo(wafer, printIDs:bool = False):

        nameStr = f'{"(wafer)":>11}                  {_nameString(wafer, printIDs)}'
        statusStr = f'{wafer.getField("status", "-", verbose = False):20}'
        stageStr = f'{wafer.getField("processStage", "-", verbose = False):20}'
        string = f'{nameStr} {stageStr} {statusStr}'
        
        print(string)

    @staticmethod
    def _printBarInfo(label:str, bar, printIDs:bool = False):

        labelStr = f'[{label}]'
        labelStr = f'{labelStr:12}'

        nameStr = f'{"(bar)":>11}   {labelStr}   {_nameString(bar, printIDs)}'
        statusStr = f'{bar.getField("status", "-", verbose = False):20}'
        stageStr = f'{bar.getField("processStage", "-", verbose = False):20}'
        string = f'{nameStr} {stageStr} {statusStr}'

        print(string)

    @staticmethod
    def _printChipInfo(label:str, chip, chipType:str, printIDs:bool = False):

        labelStr = f'[{label}]'
        labelStr = f'{labelStr:12}'

        nameStr = f'{f"({chipType})":>11}   {labelStr}   {_nameString(chip, printIDs)}'
        statusStr = f'{chip.getField("status", "-", verbose = False):20}'
        stageStr = f'{chip.getField("processStage", "-", verbose = False):20}'
        string = f'{nameStr} {stageStr} {statusStr}'

        print(string)


    def printDashboard(self, *,
        printIDs = False,
        printWafer = True,
        printBars = True,
        printChips = True,
        printTestChips = True,
        printTestCells = True,
        excludeNoneStatus = False):
        """Prints a schematic view of the status of wafers, bars, chips.
        
        Keyword arguments:
            - printIDs = False, (bool)
            - printWafer = True, (bool)
            - printBars = True, (bool)
            - printChips = True, (bool)
            - excludeNoneStatus = False (bool)
        """

        # Header
        print(f'Wafer Collation "{self.wafer.name}" - Dashboard\n')
        headerStr = f'{"chip type":11}   {"label":12}   {"Cmp. name":35} {"Process stage":20} {"Status":20}'
        print(headerStr)
        print(len(headerStr)*'-')

        if printWafer:
            self._printWaferInfo(self.wafer, printIDs)

        barsDict = {} if self.barsDict is None else self.barsDict
        chipsDict = {} if self.chipsDict is None else self.chipsDict
        testChipsDict = {} if self.testChipsDict is None else self.testChipsDict
        testCellsDict = {} if self.testCellsDict is None else self.testCellsDict

        # Table
        if printBars:
            for label, bar in barsDict.items():
                if excludeNoneStatus and bar.status is None: continue
                self._printBarInfo(label, bar, printIDs)

        if printChips:
            for label, chip in chipsDict.items():
                if excludeNoneStatus and chip.status is None: continue
                self._printChipInfo(label, chip, 'chip', printIDs)

        if printTestChips:
            for label, chip in testChipsDict.items():
                if excludeNoneStatus and chip.status is None: continue
                self._printChipInfo(label, chip, 'test chip', printIDs)

        if printTestCells:
            for label, chip in testCellsDict.items():
                if excludeNoneStatus and chip.status is None: continue
                self._printChipInfo(label, chip, 'test cell', printIDs)
        


# ------------------------------------------------------------------------------

# These other subclass perform a check on the name of the wafer to issue a
# warning if it dies not correspond. It is the only change compared to the
# base waferCollation class.


class waferCollation_Bilbao(waferCollation):

    def __init__(self, connection:mom.connection, waferName_orCmp_orID):

        super().__init__(connection, waferName_orCmp_orID)

        if not ('BI' in self.wafer.name or 'CDM' in self.wafer.name):
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Bilbao" wafer.')

    

class waferCollation_Budapest(waferCollation):

    def __init__(self, connection:mom.connection, waferName_orCmp_orID):

        super().__init__(connection, waferName_orCmp_orID)

        if 'DR' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Budapest" wafer.')


class waferCollation_Cambridge(waferCollation):

    def __init__(self, connection:mom.connection, waferName_orCmp_orID):

        super().__init__(connection, waferName_orCmp_orID)

        if 'CM' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Cambridge" wafer.')


class waferCollation_Como(waferCollation):
    def __init__(self, connection:mom.connection, waferName_orCmp_orID):

        super().__init__(connection, waferName_orCmp_orID)

        if 'CO' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Como" wafer.')

        # raise NotImplementedError('the waferCollation for the "Como" maskset has not yet been defined.')


class waferCollation_Cordoba(waferCollation):
    
    def __init__(self, connection:mom.connection, waferName_orCmp_orID):

        super().__init__(connection, waferName_orCmp_orID)

        if 'CA' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Cordoba" wafer.')

        # raise NotImplementedError('the waferCollation for the "Como" maskset has not yet been defined.')


class waferCollation_Coimbra(waferCollation):

    def __init__(self, connection:mom.connection, waferName_orCmp_orID):
        
        super().__init__(connection, waferName_orCmp_orID)

        if 'CB' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Coimbra" wafer.')



# ------------------------------------------------------------------------------
# Utilities functions
# ------------------------------------------------------------------------------


def averageSubchipScaleDataDict(dataDict_subchipScale,
                                returnValuesOnly:bool = False):
    """Given a subchip-scale datadict, it returns a chip-scale dictionary
    with averaged values.

    Non-float/int and None values are ignored.

    Notice that inner values of dataDict_subchipScale may be numbers or
    test result dictionaries, that is:

    {
        <component label 1>: {
            <location 1>: <value 1>,
            <location 2>: <value 2>,
            ...
        },
        ...
    }

    or

    {
        <component label 1>: {
            <location 1>: <test result dict 1>,
            <location 2>: <test result dict 2>,
            ...
        },
        ...
    }
    
    The output dictionary contains dataclass dictionaries ("value", "unit"),
    with the error being left out.

    If "returnValuesOnly" is set to True, only the values are returned.
    """

    def averageValues(values:list):

        values = [v for v in values if isinstance(v, int) or isinstance(v, float)]
        
        if values == []:
            return None
        
        return average(values)
    
    def averageUnits(units:list):

        log.debug(f'[averageUnits] units: {units}')
        while None in units: units.remove(None)
        
        if len(units) == 0:
            return None
        
        elif len(units) == 1:
            unit = units[0]
            if not isinstance(unit, str):
                raise ValueError(f'Cannot average if dataclass units are not equal or None.')
            return unit
        
        else:
            for unit in units[1:]:
                if unit != units[0]:
                    raise ValueError(f'Cannot average if dataclass units are not equal or None.')
            
            if not isinstance(unit, str):
                raise ValueError(f'Cannot average if dataclass units are not equal or None.')

            return unit
        

    def averageDataclasses(dataclasses:list):
        
        dcs = [dataClass.fromDictionary(dc) if not isinstance(dc, dataClass) else dc
               for dc in dataclasses]  

        value = averageValues([dd.value for dd in dcs])
        unit = averageUnits([dd.unit for dd in dcs])

        return dataClass(value, unit = unit).dictionary()

    def averageTestResults(results):

        dataclasses = []
        for result in results:

            if result is None:
                continue
            
            data = result.get('resultData')
            if data is None:
                continue

            dataclasses.append(dataClass(value = data.get('value'), unit = data.get('unit')))

        return averageDataclasses(dataclasses)
    
    
    def averageComponentDict(dic):

        if dic is None or dic == {}:
            return None

        if any([isinstance(val, dict) for val in dic.values()]):
             # test result dictionaries
            return averageTestResults(dic.values())
        else: # I assume I am dealing with numbers
            return averageValues(dic.values())
        
    dataDict = {key: averageComponentDict(d)
                for key, d in dataDict_subchipScale.items()}
    

    return dataDict


def queryWafers(connection:mom.connection, *, waferType:str = None, returnType:str = 'name'):
    """Queries beLaboratory/components for wafers.

    Args:
        connection (mongomanager.connection): The connection instance to the
            MongoDB server. 
        waferType (str, optional): If passed, it checks that the string
            "waferType" appears in the document name.
        returnType (str, optional): Can be either "name" or "wafer". If "name",
            only the name of the wafers are returned; if "wafer", the whole
            documents are returned. Defaults to "name".

    Raises:
        TypeError: If arguments are not specified correctly.
        ValueError: If arguments are not specified correctly.

    Returns:
        list[str] | list[mongomanager.wafer] | None: The list of results found,
            or None if no result is found.
    """

    _returnTypes = ['name', 'wafer']

    if not isinstance(connection, mom.connection):
        raise TypeError('"connection" must be a mongomanager.connection object.')

    if waferType is not None:
        if not isinstance(waferType, str):
            raise TypeError('"waferType" must be a string or None.')
    
    if not isinstance(returnType, str):
        raise TypeError('"returnType" must be a string.')

    if not returnType in _returnTypes:
        raise ValueError('"returnType" must be either "name" or "wafer".')
    
    query = {'type': 'wafer'}

    # Defining query parameters
    if returnType == "name":
        proj = {'name': 1}
    elif returnType == 'wafer':
        proj = None

    if waferType is not None:
        query['name'] = {"$regex": waferType}


    queryResults = mom.query(connection,query, proj,
                            "beLaboratory", "components", returnType = "native")

    if queryResults is None:
        return None

    if returnType == "name":
        names = [qr.name for qr in queryResults]
        return names
    elif returnType == "wafer":
        return queryResults


def _statusString(component):

    if not component.hasStatusLog():
        return '<No status>'
    
    entry = component.getField(['statusLog', -1], verbose = False)

    if entry is None:
        return '<No status>'

    status = entry['status']
    date = entry['dateOfChange']
    dateStr = date.strftime('(%Z) %Y/%m/%d - %H:%M:%S')

    return f'{dateStr} :: Status: "{status}"'

def _nameString(component, printIDs = False):

    if printIDs:
        if component.ID is None:
            IDstr = f'(ID: <{"-- No ID --":^22}>) '
        else:
            IDstr = f'(ID: {component.ID}) '
        
        nameStr = IDstr
    else:
        nameStr = ''

    name = component.name
    if name is None: name = "<No name>"

    symbols = component.indicatorsString()
    if symbols is not None:
        name += f' ({symbols})'

    nameStr += f'{name:35}'
    
    return nameStr



def _joinListsOrNone(*args):
    """Returns a single dictionary from a arguments that can be lists or None."""
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
    """Returns a single dictionary from a arguments that can be dictionaries or None."""
    returnDict = {}

    for arg in args:
        if arg is None:
            continue
        elif not isinstance(arg, dict):
            raise TypeError('All arguments must be dictionaries or None.')
        else:
            returnDict |= arg

    return returnDict