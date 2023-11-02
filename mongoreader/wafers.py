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

chipGoggles = gf.chipGoggleFunctions()


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


class waferCollation(c.collation):
    """A waferCollation is a class used to collect from the database a wafer
    and its related components (typically chips, bars and test structures).
    
    The collation has useful methods to extract and plot data from this
    set of components.

    Apart from the methods described below, a waferCollation has some useful
    features that can be used for retrieving and plotting information on its
    wafer/chips/bars, and to retrieve these components easily.

    """

    def __init__(self, connection:mom.connection, waferName_orCmp_orID):
        """Initialization method of the waferCollation class.

        The main purpose of this method is to retrieve the wafer, bars, and
        chip components from the database and assign them to the corresponding
        attributes of the waferCollation instance.

        This __init__ method is meant to be called __init__ of subclasses of  
        waferCollation. Some arguments have to be defined in the subclass
        __init__ and passed, otherwise exception are raised.

        Args:
            connection (mongomanager.connection): The connection instance to 
                the MongoDB server.
            waferName_orCmp_orID (str | mongomanager.component | ObjectId): The 
                collation wafer.
            database (str, optional): The MongoDB database where the wafer is
                found (needed if the wafer is not passed as a full component). 
                Defaults to 'beLaboratory'.
            collection (str, optional): The MongoDB collection where the wafer
                is found (needed if the wafer is not passed as a full 
                component). Defaults to 'components'.

        Keyword args:
            chipsKeyCriterion (callable, optional): A function that takes the
                name of a chip as argument and returns the key to be used in the
                chipsDictionary. (e.g. "2CDM0005_DR8-01" -> "DR8-01").
                Defaults to None, in which case the chip dictionary is not
                initialized.
            barsKeyCriterion (callable): As above, for the bars. Defaults to
                None, in which case the dictionary is not initialized.
            testChipsKeyCriterion (callable, optional): Works similarly to
                chipsKeyCriterion, but is used to defined the test chip dict.
                Defaults to None, in which case the test chip dictionary is not
                initialized.
            waferMaskLabel (str): To be passed by the subclass of
                waferCollation. Defaults to None, but raises an error if it
                raises ImplementationError if it is not passed as a string.
            chipsCheckNumber (int, optional): If passed, __init__ checks that
                the amount of retrieved chip corresponds to this number.
                This must include both normal and test chips.
                Defaults to None.
            chipBlueprintCheckNumber (int, optional): If passed, __init__ 
                checks that the amount of retrieved chip blueprints corresponds
                to this number. Defaults to None.
            barsCheckNumber (int, optional): If passed, __init__ 
                checks that the amount of retrieved bar components corresponds
                to this number. Defaults to None.

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
                self.collectTestCells(self.wafer, self. testCellBlueprints, self.testCellBPdict)
        
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

    def collectTestCells(self, wafer, cellBPs, cellBPsDict):

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

    def retrieveDatasheetData(self,
        resultName_orNames,
        chipGroup_orGroups,
        locationGroup:str):

        if isinstance(chipGroup_orGroups, list):
            chipGroups = chipGroup_orGroups
        else:
            chipGroups = [chipGroup_orGroups]
        
        chipSerials = []
        for group in chipGroups:
            print(f'DEBUG: group {group}')
            chipSerials += self.waferBlueprint.getWaferChipSerials(group)
        
        locationDict = {}
        for serial in chipSerials:
            chipBp = self.chipBPdict[serial]
            locNames = chipBp.getLocationNames(locationGroup)
            locationDict[serial] = locNames

        if isinstance(resultName_orNames, str):
            resultNames = [resultName_orNames]
        else:
            resultNames = resultName_orNames
        
        returnDict = {resName: {serial: None for serial in chipSerials}
                        for resName in resultNames}

        for serial in chipSerials:
            goggled = chipGoggles.datasheedData(
                            self.chipsDict[serial],
                            resultNames,
                            locationDict[serial])

            # goggled is in the form
            # {
            #       <resultName1>: {<loc1>: <data1>,  <loc1>: <data2>, ...},
            #       <resultName2>: {<loc1>: <data1>,  <loc1>: <data2>, ...},     
            #       ...
            # }
            
            for goggledResName, dataDict in goggled.items():
                returnDict[goggledResName][serial] = dataDict
        
        if isinstance(resultName_orNames, str):
            return returnDict[resultName_orNames]
        else:
            return returnDict


    def _plotStringField(self, chips, field_or_chain:str,
            groups:list = None,
            what:str = None,
            colorDict = None,
            title = None):

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
        if title is None: title = f'Field "{field_or_chain}"'
        self._plotStringField(self.allChips, field_or_chain,
            groups = self.wplt.allChipGroups,
            what = 'allChip', colorDict = colorDict,
            title = title)

    def plotStringField_chips(self, field_or_chain:str, colorDict = None, title = None):
        if title is None: title = f'Field "{field_or_chain}"'
        self._plotStringField(self.chips, field_or_chain,
            groups = self.wplt.allowedChipGroups,
            what = 'chip', colorDict = colorDict,
            title = title)

    def plotStringField_testChips(self, field_or_chain:str, colorDict = None, title = None):
        if title is None: title = f'Field "{field_or_chain}"'
        self._plotStringField(self.testChips, field_or_chain,
            groups = self.wplt.allowedTestChipGroups,
            what = 'testChips', colorDict = colorDict,
            title = title)

    def plotStringField_testCells(self, field_or_chain:str, colorDict = None, title = None):
        if title is None: title = f'Field "{field_or_chain}"'
        self._plotStringField(self.testCells, field_or_chain,
            groups = self.wplt.allowedTestCellpGroups,
            what = 'testCells', colorDict = colorDict,
            title = title)


    def plotStatus_allChips(self, colorDict = None):
        self.plotStringField_allChips('status', colorDict = colorDict, title = 'Chip status')

    def plotStatus_chips(self, colorDict = None):
        self.plotStringField_chips('status', colorDict = colorDict, title = 'Chip status')

    def plotStatus_testChips(self, colorDict = None):
        self.plotStringField_testChips('status', colorDict = colorDict, title = 'Chip status')

    def plotStatus_testCells(self, colorDict = None):
        self.plotStringField_testCells('status', colorDict = colorDict, title = 'Chip status')


    def plotProcessStage_allChips(self, colorDict = None):
        self.plotStringField_allChips('processStage', colorDict = colorDict, title = 'Process Stage')

    def plotProcessStage_chips(self, colorDict = None):
        self.plotStringField_chips('processStage', colorDict = colorDict, title = 'Process Stage')

    def plotProcessStage_testChips(self, colorDict = None):
        self.plotStringField_testChips('processStage', colorDict = colorDict, title = 'Process Stage')

    def plotProcessStage_testCells(self, colorDict = None):
        self.plotStringField_testCells('processStage', colorDict = colorDict, title = 'Process Stage')



    def plotDatasheetData(self,
        resultName:str,
        chipGroup_orGroups,
        locationGroup:str,
        *,
        dataType:str,
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
        dpi = None
        ):
        """Generates a subchip-scale plot of datasheet-ready data.
        
        Returns the dataDict used to generate the plot."""

        plt = wplt.waferPlotter(self.connection, self.waferBlueprint)
    
        dataDict = self.retrieveDatasheetData(
            resultName,
            chipGroup_orGroups,
            locationGroup
        )
        
        if isinstance(chipGroup_orGroups, str):
            chipGroups = [chipGroup_orGroups]
        else:
            chipGroups = chipGroup_orGroups

        plt.plotData_subchipScale(dataDict,
            title = resultName,
            dataType=dataType,
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

        nameStr = f'{"[wafer]":>7} {_nameString(wafer, printIDs)}'
        statusStr = _statusString(wafer)
        string = f'{nameStr} :: {statusStr}'
        
        print(string)

    @staticmethod
    def _printBarInfo(bar, printIDs:bool = False):

        nameStr = f'{"[bar]":>7} {_nameString(bar, printIDs)}'
        statusStr = _statusString(bar)
        string = f'{nameStr} :: {statusStr}'

        print(string)

    @staticmethod
    def _printChipInfo(chip, label, printIDs:bool = False):

        nameStr = f'{"[{label}]":>7} {_nameString(chip, printIDs)}'
        statusStr = _statusString(chip)
        string = f'{nameStr} :: {statusStr}'

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


# Utilities functions

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

    nameStr += f'{name:30}'
    
    return nameStr