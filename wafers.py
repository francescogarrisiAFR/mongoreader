import mongomanager as mom
# import mongomanager.info as i
import mongoreader.core as c
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
    
    The collation than has useful methods to extract and plot data from this
    set of components.
    
    A generic waferCollation is not usually instanciated directly; instead,
    for specific wafer types you should use the related sub-class.
    For instance:
        Bilbao -> waferCollation_CDM
        Budapest -> waferCollation_PAM4
        Cambridge -> waferCollation_Cambridge
        Como -> waferCollation_Como
    """

    def __init__(self, connection:mom.connection, waferName_orCmp_orID,
            database:str = 'beLaboratory', collection:str = 'components',
            *,
            waferMaskLabel:str = None,
            chipsCheckNumber:int = None,
            chipBlueprintCheckNumber:int = None,
            barsCheckNumber:int = None,
            chipsKeyCriterion:callable,
            barsKeyCriterion:callable,
            ):
        """
        connection: a mongoutils/mongomanager connection object.
        waferName_orCmp_orID: the name of the wafer / a wafer component / 
            its ID -- used to retrieve the wafer and the related components
        database -- the database where to look into (def. 'beLaboratory')
        collection -- the collection where to look into (def. 'components')

        keyword arguments:
            - chipsCheckNumber:int = None -- see .collectChips()
            - barsCheckNumber:int = None -- see .collectBars()
            - chipsKeyCriterion:callable -- see .defineChipsDict()
            - barsKeyCriterion:callable -- see .defineBarsDict()
        """

        if not isinstance(connection, mom.connection):
            raise TypeError('"connection" must be a mongomanager.connection object.')

        self.connection = connection

        if isinstance(waferName_orCmp_orID, mom.wafer):
            self.wafer = waferName_orCmp_orID
        else:
            self.wafer = self.collectWafer(waferName_orCmp_orID, database, collection)

        self.waferBlueprint = self.collectWaferBlueprint()
        
        if not isinstance(waferMaskLabel, str):
            raise TypeError('"waferMaskLabel" must be a string.')

        self.chips = self.collectChips(chipsCheckNumber)
        self.bars = self.collectBars(barsCheckNumber)
        
        self.chipsDict = self.defineChipsDict(chipsKeyCriterion)
        self.chipBPdict = self.collectChipBlueprints(chipBlueprintCheckNumber)
        
        self.barsDict = self.defineBarsDict(barsKeyCriterion)
        
        self.waferMaskLabel = waferMaskLabel

    def collectWafer(self, waferName_orID:str,
        database:str = 'beLaboratory', collection:str = 'components'):
        """Retrieves the wafer component from the database.
        
        waferName_orID -- The name (string) or ID of the wafer to be retrieved.
        database/collection (def. beLaboratory/components) -- where to look for 
        it.
        """

        if isinstance(waferName_orID, str):

            wafer = mom.queryOne(self.connection, {'name': {'$regex': waferName_orID}}, None,
            database, collection, 'native',
            verbose = False)
        
            if wafer is None:
                raise DocumentNotFound(f'Could not find a wafer from string "{waferName_orID}".')

        else:
            if not isID(waferName_orID):
                raise TypeError('"waferName_orID" must be a string or an ID.')

            wafer = mom.importWafer(waferName_orID, self.connection)
            if wafer is None:
                raise DocumentNotFound(f'Could not import a wafer from ID "{waferName_orID}".')

        log.info(f'Collected wafer "{wafer.name}"')
        return wafer

    
    def collectBars(self,
        checkNumber:int = None,
        database:str = 'beLaboratory', collection:str = 'components'):
        """Queries the database for bar object whose parent component is the wafer."""

        if checkNumber is not None:
            if not isinstance(checkNumber, int):
                raise TypeError('"checkNumber" must be a positive integer or None.')
            if checkNumber < 0:
                raise ValueError('"checkNumber" must be a positive integer or None.')

        wafID = self.wafer.ID

        with opened(self.connection):
            bars = mom.query(self.connection,
                {'parentComponentID': wafID, 'componentType': 'wafer bar'},
                None,
                database, collection, 'native',
                verbose = False)

        log.info(f'Collected {len(bars)} bars')
        for ind, bar in enumerate(bars):
            log.spare(f'   {ind}: {bar.name}')

        if checkNumber is not None:
            if len(bars) != checkNumber:
                log.warning(f'I expected to find {checkNumber} bars. Instead, I found {len(bars)}.')

        return bars


    def collectChips(self, checkNumber:int = None):
        """Looks into the wafer children components to find the chips associated
        to the wafer.
        
        checkNumber (positive int) -- If passed, the method checks that the
            number of retrieved chips is equal to checkNumber, and issues a
            warning in case it is not.

        Internally, the method calls .retrieveChildrenComponents() on the
        collation's wafer.
        """

        if checkNumber is not None:
            if not isinstance(checkNumber, int):
                raise TypeError('"checkNumber" must be a positive integer or None.')
            if checkNumber < 0:
                raise ValueError('"checkNumber" must be a positive integer or None.')

        chips = self.wafer.retrieveChildrenComponents(self.connection)

        log.info(f'Collected {len(chips)} chips')
        for ind, chip in enumerate(chips):
            log.spare(f'   {ind}: {chip.name}')

        if checkNumber is not None:
            if len(chips) != checkNumber:
                log.warning(f'I expected to find {checkNumber} chips. Instead, I found {len(chips)}.')

        return chips
        

    def collectWaferBlueprint(self):
        """Returns the wafer blueprint from self.wafer.
        
        Raises DocumentNotFound if the blueprint is not found."""

        wbp = self.wafer.retrieveWaferBlueprint(self.connection)
        if wbp is None:
            raise DocumentNotFound('Could not retrieve the wafer blueprint.')

        log.info(f'Collected wafer blueprint "{wbp.name}".')
        return wbp

    def collectChipBlueprints(self, checkNumber:int = None):
        
        bpDict = {serial: 
                mom.importOpticalChipBlueprint(chip.blueprintID,
                    self.connection)
            for serial, chip in self.chipsDict.items()}

        differentIDs = set()
        for ser, bp in bpDict.items():
            if bp is None:
                raise DocumentNotFound(f'Could not retrieve the blueprint associated to chip "{ser}".')
            differentIDs.add(mom.toStringID(bp.ID))

        if checkNumber is not None:
            if len(differentIDs) != checkNumber:
                log.warning(f'I expected to find {checkNumber} different blueprints. Instead, I found {len(differentIDs)}.')

        log.info(f'Collected {len(differentIDs)} different chip blueprints.')

        return bpDict

    def defineChipsDict(self, keyCriterion:callable):
        """Used to define a dictionary with 'chipLabel': <chipComponent>
        key-value pairs.
        
        keyCriterion: a function applied to the name (string) of the chip,
            which returns the label to be used as the key of the dictionary.
            By default, the dictionary should use the serial of the chip 
            (without the wafer name), so for instance:
            
                chip name: "2DR0001_DR8-01"
                keyCriterion: lambda s: s.split('_')[1]
            
                keyCriterion returns "DR8-01" and the dictionary would be
                defined as 
                {
                    'DR8-01': <2DR0001_DR8-01 chip>
                    'DR8-02': <2DR0001_DR8-02 chip>
                    ...
                }

        chipLabel must correspond to the 'nameSerial' field of the
        corresponding opticalChipBlueprint.
        """

        if self.chips is None:
            log.warning('Could not instanciate chipsDict as self.chips is None.')
            return {}

        return {keyCriterion(chip.name): chip for chip in self.chips}

    def defineBarsDict(self, keyCriterion:callable):
        """Used to define a dictionary with 'barLabel': <barComponent>
        key-value pairs.
        
        keyCriterion: a function applied to the name (string) of the bar,
            which returns the label to be used as the key of the dictionary.
            By default, bars are labeled following the pattern:
                <waferName>_Bar-A
                <waferName>_Bar-B
                <waferName>_Bar-C
                ...
            so, a possible keyCriterion would be:
                keyCriterion: lambda s: s.rsplit('-', maxsplit = 1)[1]
            
                keyCriterion returns "A", "B", ... and the dictionary would be
                defined as 
                {
                    'A': <<waferName>_Bar-A>
                    'B': <<waferName>_Bar-B>
                    ...
                }
        """

        if self.bars is None:
            log.warning('Could not instanciate barsDict as self.bars is None.')
            return {}

        return {keyCriterion(bar.name): bar for bar in self.bars}



    def refresh(self):
        """Refreshes all the components from the database."""
        
        self.wafer.mongoRefresh(self.connection)
        log.spare(f'Refreshed wafer "{self.wafer.name}".')

        self.waferBlueprint.mongoRefresh(self.connection)
        log.spare(f'Refreshed waferBlueprint "{self.waferBlueprint.name}".')

        with opened(self.connection):
            for chip in self.chips:
                chip.mongoRefresh(self.connection)
        log.spare(f'Refreshed chips.')

        with opened(self.connection):
            for bar in self.bars:
                bar.mongoRefresh(self.connection)
        log.spare(f'Refreshed bars.')


    # ---------------------------------------------------
    # Data retrieval methods


    def retrieveDatasheetData(self,
        resultName:str,
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

        dataDict = {serial: chipGoggles.datasheedData(
                        self.chipsDict[serial],
                        resultName,
                        locationDict[serial])
            for serial in chipSerials}

        return dataDict


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

        plt = wplt.waferPlotter(self.connection, self.waferMaskLabel)
    
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
    """Actually "Bilbao"."""

    def __init__(self, connection:mom.connection, waferName_orCmp_orID,
        database:str = 'beLaboratory', collection:str = 'components'):
    
        super().__init__(connection, waferName_orCmp_orID, database, collection,
            chipsCheckNumber=39,
            barsCheckNumber=3,
            chipBlueprintCheckNumber = 3,
            chipsKeyCriterion= lambda s: s.rsplit('_',maxsplit = 1)[1],
            barsKeyCriterion = lambda s: s.rsplit('-',maxsplit = 1)[1],
            waferMaskLabel = 'Bilbao'
        )

        if '2CDM' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Bilbao" wafer.')

    
    # -------------------------------
    # Goggle methods


    def goggleData(self, goggleFunction:callable):
        pass


    def goggleDatasheedData(self, resultName:str, locationKey:str = None):

        pass


    # -------------------------------
    # Plot methods

    def plotChipStatus(self):

        dataDict = {}
        for chip in self.chips:
            dataDict[chip.name.split('_')[1]] = chip.status
            
        plt = wplt.waferPlotter(self.waferMaskLabel)
        plt.plotData_chipScale(dataDict, dataType = 'string',
            title = 'Chip status',
            colormapName='rainbow',
            waferName = self.wafer.name,
            printChipLabels = True,
            )

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

        if '2DR' not in self.wafer.name:
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Budapest" wafer.')

    def plotChipStatus(self):

        dataDict = {}
        for chip in self.chips:
            dataDict[chip.name.split('_')[1]] = chip.status
        log.debug(f'[plotChipStatus] {dataDict}')
            
        plt = wplt.waferPlotter(self.connection, 'Budapest')
        plt.plotData_chipScale(dataDict, dataType = 'string',
            title = 'Chip status',
            colormapName='rainbow',
            waferName = self.wafer.name,
            printChipLabels = True)


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

        # if '2DR' not in self.wafer.name:
        #     log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "PAM4" wafer.')

class waferCollation_Como(waferCollation):
    def __init__(self, connection:mom.connection, waferName_orCmp_orID,
            database:str = 'beLaboratory', collection:str = 'components',
            waferMaskLabel = 'Como'):

            raise NotImplementedError('the waferCollation for the "Como" maskset has not yet been defined.')


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