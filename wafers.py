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
        """Initialization method of the waferCollation class.

        The main purpose of this method is to retrieve the wafer, bars, and
        chip components from the database and assign them to the corresponding
        attributes of the waferCollation instance.

        It also 

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
            chipsKeyCriterion (callable): A function that takes the name of a
                chip as argument and returns the key to be used in the
                chipsDictionary. (e.g. "2CDM0005_DR8-01" -> "DR8-01").
                Defaults to None.
            barsKeyCriterion (callable): As above, for the bars. Defaults to
                None.
            waferMaskLabel (str): To be passed by the subclass of
                waferCollation. Defaults to None, but raises an error if it
                raises ImplementationError if it is not passed as a string.
            chipsCheckNumber (int, optional): If passed, __init__ checks that
                the amount of retrieved chip corresponds to this number.
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

        if isinstance(waferName_orCmp_orID, mom.wafer):
            self.wafer = waferName_orCmp_orID
        else:
            self.wafer = self.collectWafer(waferName_orCmp_orID, database, collection)

        self.waferBlueprint = self.collectWaferBlueprint()
        
        if not isinstance(waferMaskLabel, str):
            raise e.ImplementationError('"waferMaskLabel" must be a string.')

        with opened(connection):
        
            self.chips = self.collectChips(chipsCheckNumber)
            self.bars = self.collectBars(barsCheckNumber)
        
            self.chipsDict = self.defineChipsDict(chipsKeyCriterion)
            self.chipBlueprints, self.chipBPdict = self.collectChipBlueprints(chipBlueprintCheckNumber)
            
            self.barsDict = self.defineBarsDict(barsKeyCriterion)
            
        self.waferMaskLabel = waferMaskLabel


    def collectWafer(self, waferName_orID,
        database:str = 'beLaboratory', collection:str = 'components'):
        """Queries the database for the specified wafer and returns it.

        Args:
            waferName_orID (str | ObjectId): The wafer name or its ID.
            database (str, optional): The database where the wafer is found.
                Defaults to 'beLaboratory'.
            collection (str, optional): The collection where the wafer is 
                found. Defaults to 'components'.

        Raises:
            DocumentNotFound: If the wafer is not found.
            TypeError: If arguments are not specified correctly.

        Returns:
            wafer: The collected wafer.
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
        """Queries the database for bar objects whose parent component is the wafer and returns them.

        Args:
            checkNumber (int, optional): If passed, it checks that the amount
                of bars found corresponds to this number. Defaults to None.
            database (str, optional): The database where the bars are  found.
                Defaults to 'beLaboratory'.
            collection (str, optional): The collection where the bars are 
                found. Defaults to 'components'.

        Raises:
            TypeError: If arguments are not specified correctly.
            ValueError: If arguments are not specified correctly.

        Returns:
            List[component]: The list of retrieved bar components. May return
                other type of mongoDocuments.
        """        

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
        """Queries the database for optical chip objects whose parent component
        is the wafer and returns them.

        Internally, the method calls .retrieveChildrenComponents() on the
        collation's wafer.
        
        checkNumber (int, optional): If passed, it checks that the amount of
            chips found corresponds to this number. Defaults to None.

        Raises:
            TypeError: If arguments are not specified correctly.
            ValueError: If arguments are not specified correctly.

        Returns:
            List[component]: The list of retrieved chip components.
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
        """Returns the wafer blueprint of the wafer.
        
        Raises:
            DocumentNotFound: If the blueprint is not found.
            
        Returns:
            waferBlueprint: The retrieved wafer blueprint document.
        """

        wbp = self.wafer.retrieveWaferBlueprint(self.connection)
        if wbp is None:
            raise DocumentNotFound('Could not retrieve the wafer blueprint.')

        log.info(f'Collected wafer blueprint "{wbp.name}".')
        return wbp


    def collectChipBlueprints(self, checkNumber:int = None):
        """Queries the database for optical chip blueprint associated to the
        collation chips.
        
        checkNumber (int, optional): If passed, it checks that the amount of
            chips blueprint found corresponds to this number. Defaults to None.

        Raises:
            TypeError: If arguments are not specified correctly.
            ValueError: If arguments are not specified correctly.
            DocumentNotFound: If any of the collation chip has no associated
                chip blueprint.

        Returns:
            (List[blueprint], dict): The list of retrieved optical chip and
                a dictionary in the form
                >>> {
                >>>     <chip serial>: <optical chip blueprint>,
                >>>     ...
                >>> }
                that associates a chip serial with its blueprint.
        """

        chipBPdict = {}
        chipBlueprints = []
        chipBlueprintIDs = []
        
        with opened(self.connection):
            for serial, chip in self.chipsDict.items():
                bp = mom.importOpticalChipBlueprint(chip.blueprintID, self.connection)
                
                if bp is None:
                    raise DocumentNotFound(f'Could not retrieve the blueprint associated to chip "{serial}".')

                chipBPdict[serial] = bp
                if bp.ID not in chipBlueprintIDs:
                    chipBlueprintIDs.append(bp.ID)
                    chipBlueprints.append(bp)

        if checkNumber is not None:
            if len(chipBlueprints) != checkNumber:
                log.warning(f'I expected to find {checkNumber} different blueprints. Instead, I found {len(chipBlueprints)}.')

        log.info(f'Collected {len(chipBlueprints)} different chip blueprints.')

        return chipBlueprints, chipBPdict

    def defineChipsDict(self, keyCriterion:callable):
        """Used to define a dictionary with 'chipLabel': <chipComponent>
        key-value pairs.
        
        Args:
            keyCriterion (callable): a function applied to the name (string) of
                the chip, which returns the label to be used as the key of the
                dictionary.
                By default, the dictionary should use the serial of the chip 
                (without the wafer name), so for instance:
                
                chip name: "2DR0001_DR8-01"
                keyCriterion:
                >>> lambda s: s.split('_')[1]
            
                keyCriterion returns "DR8-01" and the dictionary would be
                defined as 
                >>> {
                >>>     'DR8-01': <2DR0001_DR8-01 chip>
                >>>     'DR8-02': <2DR0001_DR8-02 chip>
                >>>     ...
                >>> }

                chipLabel must correspond to the 'nameSerial' field of the
                corresponding opticalChipBlueprint.

        Returns:
            dict: The optical chip dictionary.
        """

        if self.chips is None:
            log.warning('Could not instanciate chipsDict as self.chips is None.')
            return {}

        return {keyCriterion(chip.name): chip for chip in self.chips}

    def defineBarsDict(self, keyCriterion:callable):
        """Used to define a dictionary with 'barLabel': <barComponent>
        key-value pairs.
        
        Args:
            keyCriterion (callable): a function applied to the name (string) of
            the bar, which returns the label to be used as the key of the
            dictionary.

            By default, bars are labeled following the pattern:
                <waferName>_Bar-A
                <waferName>_Bar-B
                <waferName>_Bar-C
                ...
            so, a possible keyCriterion would be:
            >>> lambda s: s.rsplit('-', maxsplit = 1)[1]
            
            keyCriterion returns "A", "B", ... and the dictionary would be
            defined as 
            >>> {
            >>>     'A': <<waferName>_Bar-A>
            >>>     'B': <<waferName>_Bar-B>
            >>>     ...
            >>> }

        Returns:
            dict: The wafer bar dictionary.
        """

        if self.bars is None:
            log.warning('Could not instanciate barsDict as self.bars is None.')
            return {}

        return {keyCriterion(bar.name): bar for bar in self.bars}



    def refresh(self):
        """Refreshes from the database all the components and blueprints of
        the collation.
        
        It calls mongoRefresh() on all of them."""
        
        with opened(self.connection):
            
            self.wafer.mongoRefresh(self.connection)
            log.spare(f'Refreshed wafer "{self.wafer.name}".')

            self.waferBlueprint.mongoRefresh(self.connection)
            log.spare(f'Refreshed waferBlueprint "{self.waferBlueprint.name}".')
        
            for chip in self.chips: chip.mongoRefresh(self.connection)
            log.spare(f'Refreshed chips.')

            for bar in self.bars: bar.mongoRefresh(self.connection)
            log.spare(f'Refreshed bars.')

            for bp in self.chipBlueprints: bp.mongoRefresh(self.connection)
            log.spare(f'Refreshed chip blueprints.')


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

    def plotChipStatus(self, colorDict = None):

        dataDict = {}
        for chip in self.chips:
            dataDict[chip.name.split('_')[1]] = chip.getField('status', verbose = False)
            
        plt = wplt.waferPlotter(self.connection, self.waferMaskLabel)
        plt.plotData_chipScale(dataDict, dataType = 'string',
            title = 'Chip status',
            colormapName='rainbow',
            waferName = self.wafer.name,
            printChipLabels = True,
            colorDict=colorDict
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

        if not ('BI' in self.wafer.name or 'CDM' in self.wafer.name):
            log.warning(f'The collected wafer ("{self.wafer.name}") may not be a "Bilbao" wafer.')

    
    # -------------------------------
    # Goggle methods


    def goggleData(self, goggleFunction:callable):
        pass


    def goggleDatasheedData(self, resultName:str, locationKey:str = None):

        pass


    # -------------------------------
    # Plot methods

    

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