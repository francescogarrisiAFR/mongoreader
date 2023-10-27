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

class waferCollation(c.collation):
    """A waferCollation is a class used to collect from the database a wafer
    and its related components (typically chips, bars and test structures).
    
    The collation has useful methods to extract and plot data from this
    set of components.
    
    A generic waferCollation is not usually instanciated directly; instead,
    for specific wafer types (a type is identified in terms of the maskset of
    the wafer) you should use the related sub-class. For instance:
    - Bilbao: waferCollation_Bilbao
    - Budapest: waferCollation_Budapest
    - Cambridge: waferCollation_Cambridge
    - Como: waferCollation_Como

    Apart from the methods described below, a waferCollation has some useful
    features that can be used for retrieving and plotting information on its
    wafer/chips/bars, and to retrieve these components easily.

    """

    checkNumber_chips = None
    checkNumber_testChips = None
    checkNumber_bars = None
    checkNumber_testCells = None

    checkNumber_chipBlueprints = None
    checkNumber_testChipBlueprints = None
    checkNumber_barBlueprints = None
    checkNumber_testCellBlueprints = None

    chipLabelCriterion = None
    testChipLabelCriterion = None
    barLabelCriterion = None
    testCellLabelCriterion = None

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
                self.wafer = self.collectWafer(waferName_orCmp_orID)

            # Collecting wafer blueprint

            self.waferBlueprint = self.collectWaferBlueprint(self.wafer)
            
        
            # Collecting chip blueprints
            self.chipBlueprints, self.chipBPdict = \
                 self.collectChipBlueprints(self.waferBlueprint)

            # Collecting test chip blueprints
            self.testChipBlueprints, self.testChipBPdict = \
                 self.collectTestChipBlueprints(self.waferBlueprint)

            # Collecting bar blueprints
            self.barBlueprints, self.barBPdict = \
                 self.collectBarBlueprints(self.waferBlueprint)

            # Collecting test cell blueprints
            self.testCellBlueprints, self.testCellBPdict = \
                 self.collectTestCellBlueprints(self.waferBlueprint)

            
            # Collecting chips, testChips and bars
            self.chips, self.chipsDict, \
            self.testChips, self.testChipsDict, \
            self.bars, self.barsDict = \
                self.collectChipsalike(self.wafer,
                            self.chipBlueprints, self.chipBPdict,
                            self.testChipBlueprints, self.testChipBPdict,
                            self.barBlueprints, self.barBPdict,
                            )
            
            # Collecting testCells
            self.testCells, self.testCellsDict = \
                self.collectTestCells(self.wafer, self. testCellBlueprints, self.testCellBPdict)
        

    # --- collect methods ---

    def collectWafer(self, waferName_orID):
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
                wafer = mom.importWafer(waferName_orID, self.connection)

            if wafer is None:
                raise DocumentNotFound(f'Could not import a wafer from ID "{waferName_orID}".')

        else:
            raise TypeError(f'"waferName_orID" must be a string or an ID.')

        log.info(f'Collected wafer "{wafer.name}"')
        return wafer
    

    def collectWaferBlueprint(self, wafer):
        """Returns the wafer blueprint of the wafer.
        
        Raises:
            DocumentNotFound: If the blueprint is not found.
            
        Returns:
            waferBlueprint: The retrieved wafer blueprint document.
        """

        with mom.logMode(mom.log, 'WARNING'):
            wbp = wafer.retrieveBlueprint(self.connection)
    
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



    def collectChipBlueprints(self, waferBlueprint):
        """Returns the chip blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'chipBlueprints', 'chip blueprint')

    def collectTestChipBlueprints(self, waferBlueprint):
        """Returns the test chip blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'testChipBlueprints', 'test chip blueprint')

    def collectBarBlueprints(self, waferBlueprint):
        """Returns the test bar blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'barBlueprints', 'bar blueprint')

    def collectTestCellBlueprints(self, waferBlueprint):
        """Returns the test cell blueprints associated to the wafer, without repetitions"""
        return self._collectChipBPalikes(waferBlueprint, 'testCellBlueprints', 'test cell blueprint')
        


    def _collectChipsalike(self, what, allChips, bps, bpDict):

        expectedAmount = len(bpDict) if bpDict is not None else 0

        if bps is None:
            log.spare(f'No blueprints for {what} blueprints. No {what}s collected')
            return None, None

        bpIDs = [bp.ID for bp in bps]

        log.debug(f'[_collectChipsalike] ({what}): bpIDs: {bpIDs}')
        log.debug(f'[_collectChipsalike] ({what}): bpDict: {bpDict}')

        # Collecting chips
        chips = [chip for chip in allChips if chip.blueprintID in bpIDs]
        log.info(f'Collected {len(chips)} {what}.')
        if len(chips) != expectedAmount:
            log.warning(f'Collected {len(chips)} {what}s but {expectedAmount} were expected.')

        # Creating dictionary
        chipsDict = {}
        for chip in chips:
            label = chip.getField('_waferLabel')
            if label is not None:
                chipsDict[label] = chip
            else:
                log.warning(f'Could not retrieve wafer label for {what} "{chip}". Not included in {what}sDict.')


        # Checking label exists in the wafer
        for label in chipsDict:
            if not label in bpDict:
                log.error(f'Label "{label}" does not exist in the wafer blueprint dictionary!')
        
        if chips == []: chips = None
        if chipsDict == {}: chipsDict = None

        return chips, chipsDict


    def collectChipsalike(self, wafer,
                     chipBPs, chipBPsDict,
                     testBPs, testChipBPsDict,
                     barBPs, barBPsDict,
                     ):

        allChipsalike = wafer.retrieveChildrenComponents(self.connection)
        
        # Chips
        chips, chipsDict = \
            self._collectChipsalike('chip', allChipsalike, chipBPs, chipBPsDict)
        
        # Test chips
        testChips, testChipsDict = \
            self._collectChipsalike('test chip', allChipsalike, testBPs, testChipBPsDict)
        
        # Bars
        bars, barsDict = \
            self._collectChipsalike('bar', allChipsalike, barBPs, barBPsDict)

        return chips, chipsDict, testChips, testChipsDict, bars, barsDict

    def collectTestCells(self, wafer, cellBPs, cellBPsDict):

        testCells = wafer.retrieveTestCells(self.connection)
        
        # testCells
        testCells, testCellsDict = \
            self._collectChipsalike('test cell', testCells, cellBPs, cellBPsDict)
        
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


    def plotAllChipStatus(self, colorDict = None):

        if self.allChips is None:
            log.warning(f'The collation of wafer "{self.wafer.name}" has no allChips attribute. Nothing printed.')
            return

        dataDict = {}
        for chip in self.allChips:
            dataDict[chip.name.split('_')[1]] = chip.getField('status', verbose = False)
            
        plt = wplt.waferPlotter(self.connection, self.waferBlueprint)

        plt.plotData_chipScale(dataDict, dataType = 'string',
            title = 'Chip status',
            colormapName='rainbow',
            waferName = self.wafer.name,
            printChipLabels = True,
            colorDict=colorDict
            )

    def plotChipStatus(self, colorDict = None):

        if self.chips is None:
            log.warning(f'The collation of wafer "{self.wafer.name}" has no chips attribute. Nothing printed.')
            return

        dataDict = {}
        for chip in self.chips:
            dataDict[chip.name.split('_')[1]] = chip.getField('status', verbose = False)
            
        plt = wplt.waferPlotter(self.connection, self.waferBlueprint)

        if 'waferChips' in self.waferBlueprint.retrieveChipBlueprintGroupNames():
            chipGroups = ['waferChips']
        else:
            chipGroups = None

        plt.plotData_chipScale(dataDict, dataType = 'string',
            title = 'Chip status',
            colormapName='rainbow',
            waferName = self.wafer.name,
            printChipLabels = True,
            colorDict=colorDict,
            chipGroups = chipGroups
            )
        
    def plotTestChipStatus(self, colorDict = None):

        if self.testChips is None:
            log.warning(f'The collation of wafer "{self.wafer.name}" has no test chip attribute. Nothing printed.')
            return

        dataDict = {}
        for chip in self.testChips:
            dataDict[chip.name.split('_')[1]] = chip.getField('status', verbose = False)
            
        plt = wplt.waferPlotter(self.connection, self.waferBlueprint)

        if 'testChips' in self.waferBlueprint.getWaferChipGroupNames():
            chipGroups = ['testChips']
        else:
            chipGroups = None

        plt.plotData_chipScale(dataDict, dataType = 'string',
            title = 'Chip status',
            colormapName='rainbow',
            waferName = self.wafer.name,
            printChipLabels = True,
            colorDict=colorDict,
            chipGroups = chipGroups
            )

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
    def printWaferInfo(wafer, printIDs:bool = False):

        nameStr = f'{"[wafer]":>7} {_nameString(wafer, printIDs)}'
        statusStr = _statusString(wafer)
        string = f'{nameStr} :: {statusStr}'
        
        print(string)

    @staticmethod
    def printBarInfo(bar, printIDs:bool = False):

        nameStr = f'{"[bar]":>7} {_nameString(bar, printIDs)}'
        statusStr = _statusString(bar)
        string = f'{nameStr} :: {statusStr}'

        print(string)

    @staticmethod
    def printChipInfo(chip, printIDs:bool = False):

        nameStr = f'{"[chip]":>7} {_nameString(chip, printIDs)}'
        statusStr = _statusString(chip)
        string = f'{nameStr} :: {statusStr}'

        print(string)


    def printDashboard(self, *,
        printIDs = False,
        printWafer = True,
        printBars = True,
        printChips = True,
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
            self.printWaferInfo(self.wafer, printIDs)

        bars = [] if self.bars is None else self.bars
        chips = [] if self.chips is None else self.chips

        if printBars:
            for bar in bars:
                if bar.status is None:
                    if excludeNoneStatus:
                        continue
                self.printBarInfo(bar, printIDs)

        if printChips:
            for chip in chips:
                if chip.status is None:
                    if excludeNoneStatus:
                        continue
                self.printChipInfo(chip, printIDs)
        



class waferCollation_Bilbao(waferCollation):

    checkNumber_chips = 39
    checkNumber_testChips = 0
    checkNumber_bars = 3
    checkNumber_testCells = 0

    checkNumber_chipBlueprints = 3
    checkNumber_testChipBlueprints = 0
    checkNumber_barBlueprints = 3
    checkNumber_testCellBlueprints = 0

    chipLabelCriterion = lambda self, chip: chip.name.rsplit('_',maxsplit = 1)[1]
    testChipLabelCriterion = None
    barLabelCriterion = lambda self, bar: bar.name[-1]
    testCellLabelCriterion = None

    def __init__(self, connection:mom.connection, waferName_orCmp_orID):
    
        super().__init__(connection, waferName_orCmp_orID)

        if not ('BI' in self.wafer.name or 'CDM' in self.wafer.name):
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Bilbao" wafer.')

    

class waferCollation_Budapest(waferCollation):

    def __init__(self, connection:mom.connection, waferName_orCmp_orID,
            database:str = 'beLaboratory', collection:str = 'components'):
        
        super().__init__(connection, waferName_orCmp_orID, database, collection,
            chipsCheckNumber=69,
            chipBlueprintCheckNumber = 4,
            barsCheckNumber=6,
            chipsKeyCriterion= lambda s: s.rsplit('_',maxsplit = 1)[1],
            barsKeyCriterion = lambda s: s.rsplit('-',maxsplit = 1)[1],
            waferMaskLabel = 'Budapest'
        )

        if 'DR' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Budapest" wafer.')


class waferCollation_Cambridge(waferCollation):

    def __init__(self, connection:mom.connection, waferName_orCmp_orID,
            database:str = 'beLaboratory', collection:str = 'components'):
        
        super().__init__(connection, waferName_orCmp_orID, database, collection,
            chipsCheckNumber=63,
            barsCheckNumber=7,
            chipsKeyCriterion= lambda s: s.rsplit('_',maxsplit = 1)[1],
            barsKeyCriterion = lambda s: s.rsplit('-',maxsplit = 1)[1],
            waferMaskLabel = 'Cambridge'
        )

        if 'CM' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Cambridge" wafer.')


class waferCollation_Como(waferCollation):
    def __init__(self, connection:mom.connection, waferName_orCmp_orID,
            database:str = 'beLaboratory', collection:str = 'components'):

        super().__init__(connection, waferName_orCmp_orID, database, collection,
            chipsCheckNumber=58,
            barsCheckNumber=7,
            chipsKeyCriterion= lambda s: s.rsplit('_',maxsplit = 1)[1],
            barsKeyCriterion = lambda s: s.rsplit('-',maxsplit = 1)[1],
            waferMaskLabel = 'Como'
        )

        if 'CO' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Como" wafer.')

        # raise NotImplementedError('the waferCollation for the "Como" maskset has not yet been defined.')


class waferCollation_Cordoba(waferCollation):
    

    def __init__(self, connection:mom.connection, waferName_orCmp_orID,
            database:str = 'beLaboratory', collection:str = 'components'):
        
        def chipCriterion(chipName):
            if 'T1' in chipName or 'T2' in chipName:
                return None
            else:
                return chipName.rsplit('_', maxsplit = 1)[1]

        def testChipCriterion(chipName):
            if 'T1' in chipName or 'T2' in chipName:
                return chipName.rsplit('_', maxsplit = 1)[1]
            else:
                return None


        super().__init__(connection, waferName_orCmp_orID, database, collection,
            chipsCheckNumber=90, # 60 normal chips + 30 test chips
            chipBlueprintCheckNumber=3,
            testChipBlueprintCheckNumber=2,
            barsCheckNumber=6,
            chipsKeyCriterion= chipCriterion,
            testChipsKeyCriterion= testChipCriterion,
            barsKeyCriterion = lambda s: s.rsplit('-',maxsplit = 1)[1], # Check!
            waferMaskLabel = 'Cordoba'
        )

        if 'CA' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Cordoba" wafer.')

        # raise NotImplementedError('the waferCollation for the "Como" maskset has not yet been defined.')


class waferCollation_Coimbra(waferCollation):
    

    def __init__(self, connection:mom.connection, waferName_orCmp_orID,
            database:str = 'beLaboratory', collection:str = 'components'):
        
        def chipCriterion(chipName):
            if 'AM70' in chipName or 'QR' in chipName:
                return chipName.rsplit('_', maxsplit = 1)[1]
            else:
                return None

        def testChipCriterion(chipName):
            if 'T1' in chipName or 'T2' in chipName:
                return chipName.rsplit('_', maxsplit = 1)[1]
            else:
                return None


        super().__init__(connection, waferName_orCmp_orID, database, collection,
            chipsCheckNumber=108, # 76 normal chips + 32 test chips
            chipBlueprintCheckNumber=2,
            testChipBlueprintCheckNumber=2,
            barsCheckNumber=5,
            chipsKeyCriterion= chipCriterion,
            testChipsKeyCriterion= testChipCriterion,
            barsKeyCriterion = lambda s: s.rsplit('-',maxsplit = 1)[1], # Check!
            waferMaskLabel = 'Coimbra'
        )

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