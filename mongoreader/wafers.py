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


    def retrieveData(self,
        resultName_orNames,
        chipType_orTypes,
        chipGroup_orGroups = None,
        locationGroup_orGroups = None,
        searchDatasheetData:bool = False,
        requiredStatus:str = None,
        requiredProcessStage:str = None,
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
            chipGroup_orGroups (str | List[str], optional): The group or groups
                of chips that need to be considered. If the same group is
                defined for, e.g., chips and test chips, all these chips will be
                considered (if "chip" and "testChips" are both listed in
                "chipType_orTypes").
                Defaults to None, in which case all groups are considered.

        For each chip, results are collected using the function 
        "scoopComponentResults" (defined in mongoreader/goggleFunctions.py).
        See its documentation for the meaning of arguments not listed below."""

        if chipType_orTypes is None:
            chipTypes = ['chips']
        else:
            if isinstance(chipType_orTypes, list):
                chipTypes = chipType_orTypes
            else:
                chipTypes = [chipType_orTypes]

            for el in chipTypes:
                if not isinstance(el, str):
                    raise TypeError('"chipType_orTypes" must be a string, a list of strings, or None.')

        if not all([el in ['chips', 'testChips', 'bars', 'testCells'] for el in chipTypes]):
            raise ValueError('Allowed values for "chipType_orTypes" are "chips", "testChips", "bars", "testCells".')

        if chipGroup_orGroups is None:
            chipGroups = None
        else:
            if isinstance(chipGroup_orGroups, list):
                chipGroups = chipGroup_orGroups
            else:
                chipGroups = [chipGroup_orGroups]

            for el in chipGroups:
                if not isinstance(el, str):
                    raise TypeError('"chipGroup_orGroups" must be a string, a list of strings or None.')
        
        if locationGroup_orGroups is None:
            locationGroups = None
        else:
            if isinstance(locationGroup_orGroups, list):
                locationGroups = locationGroup_orGroups
            else:
                locationGroups = [locationGroup_orGroups]

            for el in locationGroups:
                if not isinstance(el, str):
                    raise TypeError('"locationGroup_orGroups" must be a string, a list of strings or None.')
                
        
        
        # If chipGroup_orGroups is None, I collect all groups

        # Collecting all relevant serials (labels)
        chipSerials = []
        for chipType in chipTypes:

            if chipType == 'chips':
                
                chipTypeGroups = self.waferBlueprint.ChipBlueprints.retrieveGroupNames()

                if chipGroups is None:
                    groupsToScoop = chipTypeGroups
                else:
                    groupsToScoop = [g for g in chipTypeGroups if g in chipGroups]

                for group in groupsToScoop:
                    mom.log.debug(f'chipType-group: {chipType}-{group}')

                    newSerials = self.waferBlueprint.ChipBlueprints.retrieveGroupLabels(group)
                    if newSerials is not None: chipSerials += newSerials

            if chipType == 'testChips':
                
                chipTypeGroups = self.waferBlueprint.ChipBlueprints.retrieveGroupNames()

                if chipGroups is None:
                    groupsToScoop = chipTypeGroups
                else:
                    groupsToScoop = [g for g in chipTypeGroups if g in chipGroups]
                
                for group in groupsToScoop:
                    mom.log.debug(f'chipType-group: {chipType}-{group}')

                    newSerials = self.waferBlueprint.TestChipBlueprints.retrieveGroupLabels(group)
                    if newSerials is not None: chipSerials += newSerials
            
            if chipType == 'bars':
                
                chipTypeGroups = self.waferBlueprint.ChipBlueprints.retrieveGroupNames()

                if chipGroups is None:
                    groupsToScoop = chipTypeGroups
                else:
                    groupsToScoop = [g for g in chipTypeGroups if g in chipGroups]

                for group in groupsToScoop:
                    mom.log.debug(f'chipType-group: {chipType}-{group}')

                    newSerials = self.waferBlueprint.BarBlueprints.retrieveGroupLabels(group)
                    if newSerials is not None: chipSerials += newSerials
            
            if chipType == 'testCells':
                
                chipTypeGroups = self.waferBlueprint.ChipBlueprints.retrieveGroupNames()

                if chipGroups is None:
                    groupsToScoop = chipTypeGroups
                else:
                    groupsToScoop = [g for g in chipTypeGroups if g in chipGroups]
                
                for group in groupsToScoop:
                    mom.log.debug(f'chipType-group: {chipType}-{group}')

                    newSerials = self.waferBlueprint.TestCellBlueprints.retrieveGroupLabels(group)
                    if newSerials is not None: chipSerials += newSerials

        if chipSerials == []:
            mom.log.warning(f'No labels associated to groups {chipGroups}')
        
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
                            requiredStatus,
                            requiredProcessStage,
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


    def retrieveAveragedData(self,
        resultName_orNames,
        chipType_orTypes,
        chipGroup_orGroups = None,
        locationGroup_orGroups = None,
        searchDatasheetData:bool = False,
        requiredStatus:str = None,
        requiredProcessStage:str = None,
        requiredTags:list = None,
        tagsToExclude:list = None) -> dict:
        """Works like .retrieveData, but values are averaged to the chip-scale
            level."""

        dataDict_subchip = self.retrieveData(resultName_orNames,
            chipType_orTypes,
            chipGroup_orGroups,
            locationGroup_orGroups,
            searchDatasheetData,
            requiredStatus,
            requiredProcessStage,
            requiredTags,
            tagsToExclude)
        
        dataDict = averageSubchipScaleDataDict(dataDict_subchip)

        return dataDict
        


    def _plotStringField(self, chips, field_or_chain:str,
            groups:list = None,
            what:str = None,
            colorDict = None,
            title = None):
        """Retrieve a given field from all "chips" and generates a plot."""

        if chips is None:
            if what is not None:
                log.warning(f'{what}s is None. Cannot plot.')
                return

        dataDict = {chip['_waferLabel']: chip.getField(field_or_chain, verbose = False)
            for chip in chips}

        self.wplt.plotData_chipScale(dataDict, dataType = 'string',
            title = title,
            colormapName='rainbow',
            waferName = self.wafer.name,
            printChipLabels = True,
            colorDict=colorDict,
            chipGroups=groups,
            )


    def plotStringField_allChips(self, field_or_chain:str, colorDict = None, title = None):
        """Plots a given stringField for all the chips (normal and test chips)."""
        if title is None: title = f'Field "{field_or_chain}"'
        self._plotStringField(self.allChips, field_or_chain,
            groups = self.wplt.allChipGroups,
            what = 'allChip', colorDict = colorDict,
            title = title)

    def plotStringField_chips(self, field_or_chain:str, colorDict = None, title = None):
        """Plots a given stringField for the chips (normal chips only)."""
        if title is None: title = f'Field "{field_or_chain}"'
        self._plotStringField(self.chips, field_or_chain,
            groups = self.wplt.allowedChipGroups,
            what = 'chip', colorDict = colorDict,
            title = title)

    def plotStringField_testChips(self, field_or_chain:str, colorDict = None, title = None):
        if title is None: title = f'Field "{field_or_chain}"'
        """Plots a given stringField for all the test chips."""

        self._plotStringField(self.testChips, field_or_chain,
            groups = self.wplt.allowedTestChipGroups,
            what = 'testChips', colorDict = colorDict,
            title = title)

    def plotStringField_testCells(self, field_or_chain:str, colorDict = None, title = None):
        """Plots a given stringField for all the test cells."""

        if title is None: title = f'Field "{field_or_chain}"'
        self._plotStringField(self.testCells, field_or_chain,
            groups = self.wplt.allowedTestCellpGroups,
            what = 'testCells', colorDict = colorDict,
            title = title)


    def plotStatus_allChips(self, colorDict = None):
        """Plots the field "status" for all the chips (normal and test chips)."""
        self.plotStringField_allChips('status', colorDict = colorDict, title = 'Chip status')

    def plotStatus_chips(self, colorDict = None):
        """Plots the field "status" for the chips (normal chips only)."""
        self.plotStringField_chips('status', colorDict = colorDict, title = 'Chip status')

    def plotStatus_testChips(self, colorDict = None):
        """Plots the field "status" for the test chips."""
        self.plotStringField_testChips('status', colorDict = colorDict, title = 'Chip status')

    def plotStatus_testCells(self, colorDict = None):
        """Plots the field "status" for the test cells."""
        self.plotStringField_testCells('status', colorDict = colorDict, title = 'Chip status')


    def plotProcessStage_allChips(self, colorDict = None):
        """Plots the field "processStage" for all the chips (normal and test chips)."""
        self.plotStringField_allChips('processStage', colorDict = colorDict, title = 'Process Stage')

    def plotProcessStage_chips(self, colorDict = None):
        """Plots the field "processStage" for the chips (normal chips only)."""
        self.plotStringField_chips('processStage', colorDict = colorDict, title = 'Process Stage')

    def plotProcessStage_testChips(self, colorDict = None):
        """Plots the field "processStage" for the test chips."""
        self.plotStringField_testChips('processStage', colorDict = colorDict, title = 'Process Stage')

    def plotProcessStage_testCells(self, colorDict = None):
        """Plots the field "processStage" for the test cells."""
        self.plotStringField_testCells('processStage', colorDict = colorDict, title = 'Process Stage')


    def plotResults(self,
        resultName:str,
        locationGroup:str,
        chipType_orTypes = None,
        chipGroup_orGroups = None,
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
            chipGroup_orGroups (str | List[str], optional): The group or groups
                of chips that need to be considered. If the same group is
                defined for, e.g., chips and test chips, all these chips will be
                considered (if "chip" and "testChips" are both listed in
                "chipType_orTypes").
                Defaults to None, in which case all groups are considered.
            

        Keyword arguments (**kwargs):
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
            chipType_orTypes == 'chips'

            if chipGroup_orGroups is None:
                chipGroup_orGroups = self.waferBlueprint.ChipBlueprints.retrieveGroupNames()
        

        dataDict = self.retrieveData(
            resultName,
            chipType_orTypes,
            chipGroup_orGroups,
            locationGroup,
            **kwargs
        )
        
        if isinstance(chipGroup_orGroups, str):
            chipGroups = [chipGroup_orGroups]
        else:
            chipGroups = chipGroup_orGroups

        plt.plotData_subchipScale(dataDict,
            title = resultName,
            NoneColor = NoneColor,
            colormapName=colormapName,
            dataRangeMin = dataRangeMin,
            dataRangeMax = dataRangeMax,
            clippingHighColor = clippingHighColor,
            clippingLowColor = clippingLowColor,
            BackColor = BackColor,
            waferName = self.wafer.name,
            chipGroups = chipGroups,
            colorbarLabel = colorbarLabel,
            printChipLabels = printChipLabels,
            chipLabelsDirection = chipLabelsDirection,
            dpi = dpi,
            )

        return dataDict


    def plotAveragedResults(self,
        resultName:str,
        locationGroup:str,
        chipType_orTypes = None,
        chipGroup_orGroups = None,
        *,
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
        dpi = None,
        **kwargs):
        """Works like plotResults, but data is first averaged to a chip-scale
        level."""

        plt = wplt.waferPlotter(self.connection, self.waferBlueprint)
    
        if chipType_orTypes is None:
            chipType_orTypes == 'chips'

            if chipGroup_orGroups is None:
                chipGroup_orGroups = self.waferBlueprint.ChipBlueprints.retrieveGroupNames()
        

        dataDict = self.retrieveAveragedData(
            resultName,
            chipType_orTypes,
            chipGroup_orGroups,
            locationGroup,
            **kwargs
        )
        
        if isinstance(chipGroup_orGroups, str):
            chipGroups = [chipGroup_orGroups]
        else:
            chipGroups = chipGroup_orGroups

        plt.plotData_chipScale(dataDict,
            title = resultName + ' (Averaged)',
            NoneColor = NoneColor,
            colormapName=colormapName,
            dataRangeMin = dataRangeMin,
            dataRangeMax = dataRangeMax,
            clippingHighColor = clippingHighColor,
            clippingLowColor = clippingLowColor,
            BackColor = BackColor,
            waferName = self.wafer.name,
            chipGroups = chipGroups,
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

        nameStr = f'{"[wafer]":>11}   {_nameString(wafer, printIDs)}'
        statusStr = f'{wafer.getField("status", "<No status>", verbose = False):20}'
        stageStr = f'{wafer.getField("processStage", "<No proc. stage>", verbose = False):20}'
        string = f'{nameStr} {stageStr} {statusStr}'
        
        print(string)

    @staticmethod
    def _printBarInfo(bar, printIDs:bool = False):

        nameStr = f'{"[bar]":>11}   {_nameString(bar, printIDs)}'
        statusStr = f'{bar.getField("status", "<No status>", verbose = False):20}'
        stageStr = f'{bar.getField("processStage", "<No proc. stage>", verbose = False):20}'
        string = f'{nameStr} {stageStr} {statusStr}'

        print(string)

    @staticmethod
    def _printChipInfo(chip, label, printIDs:bool = False):

        nameStr = f'{f"[{label}]":>11}   {_nameString(chip, printIDs)}'
        statusStr = f'{chip.getField("status", "<No status>", verbose = False):20}'
        stageStr = f'{chip.getField("processStage", "<No proc. stage>", verbose = False):20}'
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

        if printWafer:
            self._printWaferInfo(self.wafer, printIDs)

        bars = [] if self.bars is None else self.bars
        chips = [] if self.chips is None else self.chips
        testChips = [] if self.testChips is None else self.testChips
        testCells = [] if self.testCells is None else self.testCells

        if printBars:
            for bar in bars:
                if bar.status is None:
                    if excludeNoneStatus:
                        continue
                self._printBarInfo(bar, printIDs)

        if printChips:
            for chip in chips:
                if chip.status is None:
                    if excludeNoneStatus:
                        continue
                self._printChipInfo(chip, 'chip', printIDs)

        if printTestChips:
            for chip in testChips:
                if chip.status is None:
                    if excludeNoneStatus:
                        continue
                self._printChipInfo(chip, 'test chip', printIDs)

        if printTestCells:
            for chip in testCells:
                if chip.status is None:
                    if excludeNoneStatus:
                        continue
                self._printChipInfo(chip, 'test cell', printIDs)
        


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