import mongomanager as mom
# import mongomanager.info as i
import mongoreader.core as c
import mongoreader.errors as e
import mongoreader.plotting.waferPlotting as wplt
import mongoreader.gogglesFunctions as gf

from mongoutils import isID
from mongoutils.connections import opened
from mongomanager import log

from mongomanager.errors import DocumentNotFound

from numpy import average

chipGoggles = gf.chipGoggleFunctions()


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

class _Datasheet(_attributeClass):
    """Attribute class to apply Datasheet methods to wafer collations"""
    
    def printHelpInfo(self):
        pass


    def retrieveData(self,
                    resultName:str,
                    locationGroup:str,
                    requiredTags:list = None,
                    tagsToExclude:list = None,
                    chipType_orTypes = None,
                    chipGroupsDict:dict = None,
                    *,
                    datasheetIndex:int = None
                ):
        

        if chipType_orTypes is None:
            chipTypes = ['chips']
        else:
            chipTypes = [chipType_orTypes] if not isinstance(chipType_orTypes, list) else chipType_orTypes
        
        cmpLabels = self._obj._selectLabels(chipTypes, chipGroupsDict)
        


        # Scooping results

        dataDict = {}

        for label in cmpLabels:
            cmp = self._obj._allComponentsDict[label]

            # Retrieving all possible locations and defining sub-dict
            bp = self._obj._allComponentBPdict[label]
            locations = bp.Locations.retrieveGroupElements(locationGroup)
            subDataDict = {loc: None for loc in locations}

            # Scooping results from datasheet
            scoopedResults = cmp.Datasheet.scoopResults(
                        resultName,
                        requiredTags,
                        tagsToExclude,
                        locations = locations,
                        datasheetIndex = datasheetIndex,
                        verbose = False
                    )
            
            if scoopedResults is None: # Early exit
                dataDict[label] = subDataDict
                continue

            # Collecting the results in the sub-dict

            for resDict in scoopedResults:

                # resDict = {
                #     "resultName": "IL",
                #     "location": "MZ-X",
                #     "requiredTags": [
                #         "1550nm",
                #         "25C"
                #     ],
                #     "tagsToExclude": null,
                #     "resultData": {
                #         "value": 13.908280000000003,
                #         "error": 0.4,
                #         "unit": "dB"
                #     },
                #     "testReportID": "6545f5752687e9d6549ec0b6"
                # }

                loc = resDict.get('location')
                if loc is None:
                    log.warning('A location for a scooped result is None. Skipped.')
                    continue

                resValue = resDict.get('resultData')
                if resValue is not None:
                    resValue = resValue.get('value')
                
                if resValue is None:
                    log.warning('A result value for a scooped result is None. Skipped.')
                    continue

                if subDataDict[loc] is not None:
                    log.warning(f'Multiple results for component "{cmp.name}" - location "{loc}"')
                
                subDataDict[loc] = resValue
        
            dataDict[label] = subDataDict

        # Returning dataDictinoary
        return dataDict

    def retrieveAveragedData(self,
            resultName:str,
            locationGroup:str,
            requiredTags:list = None,
            tagsToExclude:list = None,
            chipType_orTypes = None,
            chipGroupsDict:dict = None,
            *,
            datasheetIndex:int = None):
        
        dd = self.retrieveData(
                    resultName,
                    locationGroup,
                    requiredTags,
                    tagsToExclude,
                    chipType_orTypes,
                    chipGroupsDict,
                    datasheetIndex = datasheetIndex)

        return averageSubchipScaleDataDict(dd)

    def plotData(self,
            resultName:str,
            locationGroup:str,
            requiredTags:list = None,
            tagsToExclude:list = None,
            chipType_orTypes = None,
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
            printChipLabels:bool = False,
            chipLabelsDirection:str = None,
            title:str = None,
            dpi = None,
            **kwargs
            ):
        """Creates a subchip-scale plot of given results.
        
        For more info on not-listed arguments see waferPlotter.plotData_subchipScale()
        (defined in mongoreader/plotting/waferPlotting.py)

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
    
        if chipType_orTypes is None:
            chipTypes = ['chips']
        elif not isinstance(chipType_orTypes, list):
            chipTypes = [chipType_orTypes]
        else:
            chipTypes = chipType_orTypes

        log.debug(f'[plotResults] chipTypes: {chipTypes}')

        dataDict = self.retrieveData(
            resultName,
            locationGroup,
            requiredTags,
            tagsToExclude,
            chipType_orTypes,
            chipGroupsDict,
            datasheetIndex = datasheetIndex)
        
        if title is None: title = resultName

        plt.plotData_subchipScale(dataDict,
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

    def plotAveragedData(self,
            resultName:str,
            locationGroup:str,
            requiredTags:list = None,
            tagsToExclude:list = None,
            chipType_orTypes = None,
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
            printChipLabels:bool = False,
            chipLabelsDirection:str = None,
            title:str = None,
            dpi = None,
            **kwargs
            ):
        """Creates a subchip-scale plot of given results.
        
        For more info on not-listed arguments see waferPlotter.plotData_subchipScale()
        (defined in mongoreader/plotting/waferPlotting.py)

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
    
        if chipType_orTypes is None:
            chipTypes = ['chips']
        elif not isinstance(chipType_orTypes, list):
            chipTypes = [chipType_orTypes]
        else:
            chipTypes = chipType_orTypes

        log.debug(f'[plotResults] chipTypes: {chipTypes}')

        dataDict = self.retrieveAveragedData(
            resultName,
            locationGroup,
            requiredTags,
            tagsToExclude,
            chipType_orTypes,
            chipGroupsDict,
            datasheetIndex = datasheetIndex)
        
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



@_attributeClassDecoratorMaker(_Datasheet)
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


    def _collectChipBPalikes(self, waferBlueprint, WBPfield:str, what:str):

        log.debug(f'[_collectChipBPalikes] field: "{WBPfield}".')

        chipBlueprintDicts = waferBlueprint.getField(WBPfield, verbose = None)

        if chipBlueprintDicts is None:
            log.spare(f'No blueprints associated to field "{WBPfield}" of waferBlueprint "{waferBlueprint.name}".')
            return None, None

        bpDict_labelsIDs = {dic['label']: dic['ID'] for dic in chipBlueprintDicts}

        IDs = []
        for dic in chipBlueprintDicts:
            if dic['ID'] not in IDs:
                IDs.append(dic['ID'])

        bpDict_IDs = {}
        for ID in IDs:
            bp = mom.queryOne(self.connection, {'_id': mom.toObjectID(ID)}, None,
                    mom.blueprint.defaultDatabase,
                    mom.blueprint.defaultCollection,
                    returnType = 'native', verbose = False)
            if bp is None:
                raise DocumentNotFound(f'Could not rietrieve blueprint with ID "{ID}".')
            bpDict_IDs[ID] = bp

        bps = list(bpDict_IDs.values())

        bpDict_labelsBPs = {label: bpDict_IDs[bpDict_labelsIDs[label]] for label in bpDict_labelsIDs}
            
        for bp in bps:                
            if not isinstance(bp, mom.blueprint):
                log.warning(f'Some documents associated to field "{WBPfield}" of waferBlueprint "{waferBlueprint.name}" are not blueprints.')
            
        if bps == []: bps = None
        if bpDict_labelsBPs == {}: bpDict_labelsBPs = None


        # Checking amount

        amount = len(bps) if bps is not None else 0  
        expected = len(IDs)
        log.info(f'Collected {amount} {what}s.')
        if amount != expected:
            log.warning(f'Collected {amount} {what}s but {expected} were expected.')
        
        return bps, bpDict_labelsBPs



    def _collectChipBlueprints(self, waferBlueprint):
        """Returns the chip blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'chipBlueprints', 'chip blueprint')

    def _collectTestChipBlueprints(self, waferBlueprint):
        """Returns the test chip blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'testChipBlueprints', 'test chip blueprint')

    def _collectBarBlueprints(self, waferBlueprint):
        """Returns the test bar blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'barBlueprints', 'bar blueprint')

    def _collectTestCellBlueprints(self, waferBlueprint):
        """Returns the test cell blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'testCellBlueprints', 'test cell blueprint')
        


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

        allChipsalike = wafer.ChildrenComponents.retrieveElements(self.connection)
        
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

        testCells = wafer.TestCells.retrieveElements(self.connection)
        
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


    def _selectLabels(self, chipTypes:list, chipGroupsDict:dict):
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

        for chipType, chipGroups in chipGroupsDict.items():

            attributeClass = self._waferBlueprintAttributeClassFromType(chipType)

            if chipGroups is None:
                newLabels = attributeClass.retrieveLabels()

            else:
                newLabels = []
                for group in chipGroups:
                    newLabels += attributeClass.retrieveGroupLabels(group)

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


    def retrieveTestData(self,
        resultName_orNames,
        chipType_orTypes,
        chipGroupsDict:dict = None,
        locationGroup_orGroups = None,
        searchDatasheetData:bool = False,
        requiredStatus_orList = None,
        requiredProcessStage_orList = None,
        requiredTags:list = None,
        tagsToExclude:list = None) -> dict:
        """Returns a dictionary containing results collected from the wafer
        collation.

        Args:
            resultName_orNames (str | List[str]): The name(s) of the result(s)
                to be collected.
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
                Not all <chipType1> must be present within the dictionary.

        For each chip, results are collected using the function 
        "scoopComponentResults" (defined in mongoreader/goggleFunctions.py).
        See its documentation for the meaning of arguments not listed below."""

        if not isinstance(locationGroup_orGroups, list):
            locationGroups = [locationGroup_orGroups]
        else:
            locationGroups = locationGroup_orGroups


        if chipType_orTypes is None:
            chipTypes = ['chips']
        else:
            if isinstance(chipType_orTypes, list):
                chipTypes = chipType_orTypes
            else:
                chipTypes = [chipType_orTypes]
                

        chipSerials = self._selectLabels(chipTypes, chipGroupsDict)
        
        if chipSerials is None:
            mom.log.warning(f'No chip serials have been determined!')
            return None
        
        locationDict = {}
        for serial in chipSerials:
            locationDict[serial] = []
            bp = self._allComponentBPdict[serial]

            for locGroup in locationGroups:
                locNames = bp.Locations.retrieveGroupElements(locGroup)
                if locNames is not None:
                    locationDict[serial] += locNames

        if isinstance(resultName_orNames, str):
            resultNames = [resultName_orNames]
        else:
            resultNames = resultName_orNames
        
        returnDict = {resName: {serial: None for serial in chipSerials}
                        for resName in resultNames}

        for serial in chipSerials:
            goggled = chipGoggles.scoopComponentResults(
                            self._allComponentsDict[serial],
                            resultNames,
                            locationDict[serial], # All location names
                            searchDatasheetData,
                            requiredStatus_orList,
                            requiredProcessStage_orList,
                            requiredTags,
                            tagsToExclude)

            # goggled is in the form
            # {
            #       <resultName1>: {<loc1>: <data1>,  <loc2>: <data2>, ...},
            #       <resultName2>: {<loc1>: <data1>,  <loc2>: <data2>, ...},     
            #       ...
            # }
            
            for goggledResName, dataDict in goggled.items():
                returnDict[goggledResName][serial] = dataDict
        
        if isinstance(resultName_orNames, str):
            return returnDict[resultName_orNames]
        else:
            return returnDict


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
    


    def plotTestResults(self,
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
        printChipLabels:bool = False,
        chipLabelsDirection:str = None,
        title:str = None,
        dpi = None,
        **kwargs
        ):
        """Creates a subchip-scale plot of given results.
        
        For more info on not-listed arguments see waferPlotter.plotData_subchipScale()
        (defined in mongoreader/plotting/waferPlotting.py)

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

        plt = wplt.waferPlotter(self.connection, self.waferBlueprint)
    
        if chipType_orTypes is None:
            chipTypes = ['chips']
        elif not isinstance(chipType_orTypes, list):
            chipTypes = [chipType_orTypes]
        else:
            chipTypes = chipType_orTypes

        log.debug(f'[plotResults] chipTypes: {chipTypes}')

        dataDict = self.retrieveTestData(
            resultName,
            chipTypes,
            chipGroupsDict,
            locationGroup,
            **kwargs
        )
        
        if title is None: title = resultName

        plt.plotData_subchipScale(dataDict,
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

        return dataDict


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


def averageSubchipScaleDataDict(dataDict_subchipScale):
    """Given a subchip-scale datadict, it returns a chip-scale dictionary
    with averaged values.
    
    Non-float/int and None values are ignored."""

    def averageValues(values):

        values = [v for v in values if isinstance(v, int) or isinstance(v, float)]
        
        if values == []:
            return None
        
        return average(values)

    dataDict = {key: averageValues(d.values())
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