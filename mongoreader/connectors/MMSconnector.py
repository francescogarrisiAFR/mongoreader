from mongoutils import queryUtils as qu
import mongoreader.wafers as morw
import mongoreader.modules as morm
from datautils import dataClass
import mongomanager as mom
from mongomanager.errors import FieldNotFound
from mongomanager import log
from pandas import DataFrame, concat
from typing import TypedDict
from pathlib import Path
from abc import ABC, abstractmethod
from dataclasses import dataclass

def _acronymsFromDSdefinition(DSdefintion:list, locGroupDict:dict):
    """Generates the ".out"-like list of acronyms for a datasheet definition.
    
    The Datasheet Definition is a dictionary as defined in
    monogmanager.blueprints. The "locGroupDict" dictionary assciates associates
    location groups used in the DSdefinition to the corresponding list of
    locations associated to it. It is typically retrived using

    `blueprint.Datasheet. [CHECk]`

    "locGroupDict" is in the form:

    >>> {
    >>>     <locationGroup1>: [<loc.1>, <loc.2>, ...],
    >>>     <locationGroup2>: [<loc.1>, <loc.2>, ...],
    >>>     <locationGroup3>: [<loc.1>, <loc.2>, ...],
    >>>     ...
    >>> }

    Args:
        DSdefintion (List[dict]): The datasheet definition.
        locGroupDict (dict): The group-locations mapping dictionary.

    Returns:
        List[str]: The list of acronyms.
    """        

    acronyms = []
    for entry in DSdefintion:
        resName = entry['resultName']
        locations = locGroupDict[entry['locationGroup']]
        reqTags = entry['tagFilters']['required']

        for loc in locations:
            acronym = '_'.join([resName, loc]+reqTags)
            acronyms.append(acronym)
    
    return acronyms


def acronymsFromBlueprint(blueprint:mom.blueprint) -> list:
    """Returns the list of acronyms for a .out table retrieving the Datasheet
    Definition from a blueprint.

    Args:
        blueprint (mom.blueprint): The blueprint containing the datasheet
            defitinion.

    Raises:
        TypeError: If blueprint is not a mongomanager.blueprint object.

    Returns:
        List[str] | None: The list of acronyms
    """    

    if not isinstance(blueprint, mom.blueprint):
        raise TypeError(f'"blueprint" must be a mongomanager blueprint object.')

    DSDefinition = blueprint.getDatasheetDefinition()
    groupsDict = blueprint.Locations.retrieveGroupsDict()

    if DSDefinition is None:
        return None
    if groupsDict is None:
        return None
    
    return _acronymsFromDSdefinition(DSDefinition, groupsDict)


def _spawnEmptyDataFrame(columnNames:list) -> DataFrame:
    """Generates an Empty DataFrame whose columns are those passed as argument

    Args:
        columnNames (list[str] | None): The list of strings representing the
            DataFrame's column names.

    Returns:
        DataFrame | None: The returned DataFrame, or None if no columns are
            passed.
    """    
    
    if columnNames is None or columnNames == []:
        log.warning('The generated dataframe has no columns.')
        columnNames = []

    return DataFrame(None, columns=columnNames, index = [0])


def _spawnEmptyDotOutDataframe(acronyms:list) -> DataFrame:
    """Returns an empty DataFrame that can be used to generate a dot-out table.

    Args:
        acronyms (list[str], optional): The list of acronyms of the dot-out
            table.

    Returns:
        DataFrame: The empty DotOut DataFrame.
    """    

    if acronyms is None: acronyms = []
    if not isinstance(acronyms, list):
        raise TypeError(f'"acronyms" must be a list of strings.')
    if not all([isinstance(acr, str) for acr in acronyms]):
        raise TypeError(f'"acronyms" must be a list of strings.')

    return _spawnEmptyDataFrame(
        ['component', 'componentID', 'status', 'processStage',
        'earliestTestDate', 'latestTestDate'] + acronyms)


def generateComponentDotOutData(component, connection) -> None:
    """Uses the datasheet definition found in a component's blueprint to
    generate a new datasheet and store it in the component.
    
    The data is NOT uploaded to the database."""

    component.Datasheet.createAndStoreDatasheet(connection,
                                requiredProcessStages = None,
                                requiredStati = None)


def generateAndSaveComponentDotOutData(component, connection) -> None:
    """Uses the datasheet definition found in a component's blueprint to
    generate a new datasheet and store it in the component.

    The data is then uploaded to the database (the component is mongoReplaced).
    """

    component.mongoRefresh(connection)
    component.Datasheet.createAndStoreDatasheet(connection,
                                requiredProcessStages = None,
                                requiredStati = None)
    component.mongoReplace(connection)


def retrieveComponentDotOutData(component:mom.component)-> DataFrame:
    """This function returns from the component the data suitable for populating
    the Dot-Out table.
    
    Currently it retrieves data from the last datasheetData defined for the
    compnent.

    Returns:
        DataFrame | None: The returned data, or None if not found.
    """

    # First, I regenerate the Datasheet data
    
    DF = component.Datasheet.retrieveData(returnDataFrame = True, verbose = False)
    return DF


def dotOutDataFrame(emptyDotOutDataFrame:DataFrame,
                    component:mom.component,
                    componentDotOutData:DataFrame,
                    *,
                    allResultDigits:bool = True,
                    scientificNotationThreshold:float = 10**9,
                ) -> DataFrame:
    """Returns a dot-out dataframe for the given component, using the empty
    database as reference for the fields.

    Args:
        emptyDotOutDataFrame (DataFrame): The empty DataFrame to be populated
            with data from component. See spawnEmptyDotOutDataframe().
        component (mom.component): The component from which are retrieved the
            data that go to populate the DataFrame.
        componentDotOutData (DataFrame): The DataFrame containing the data
            retrieved from the component and used to populate the dot-out table.
    
    Keyword Args:
        allResultDigits (bool): If True, all the digits for results are reported,
            otherwise the numbers are rounded. Defaults to False.
        scientificNotationThreshold (float, optional): The threshold for the
            absolute value of numbers over which they are reported in scientific
            notation. Defaults to 10**9.

    Returns:
        DataFrame: The dot-out DataFrame for the component.
    """

    # Type Checks

    if not isinstance(component, mom.component):
        raise TypeError(f'"component" must be a mongomanager.component object (it is {type(component)}).')
    
    if not isinstance(emptyDotOutDataFrame, DataFrame):
        raise TypeError(f'"emptyDotOutDataFrame" must be a DataFrame (it is {type(emptyDotOutDataFrame)}).')
    
    if not isinstance(componentDotOutData, DataFrame):
        raise TypeError(f'"componentDotOutData" must be a DataFrame (it is {type(componentDotOutData)}).')

    rowDict = {}
    # General information
    rowDict = {
        'component': component.getField('name', verbose = False),
        'componentID': component.ID,
        'status': component.getField('status', verbose = False),
        'processStage': component.getField('processStage', verbose = False),
        'earliestTestDate': None, # To be set later
        'latestTestDate': None, # To be set later
    }

    # Populating the acronym fields
    if componentDotOutData is not None:
        
        componentDotOutData = componentDotOutData.reset_index()  # make sure indexes pair with number of rows

        earliestTestDate = None
        latestTestDate = None

        for _, r in componentDotOutData.iterrows():

            date = r.get('executionDate')
            
            if date is not None:
                if earliestTestDate is None: earliestTestDate = date
                if latestTestDate is None: latestTestDate = date

                if date < earliestTestDate: earliestTestDate = date
                if date > latestTestDate: latestTestDate = date

            resName = r.get('resultName')
            loc = r.get('location')
            reqTags = r.get('requiredTags')

            resValue = r.get('resultValue')

            if resValue is None:
                resValue = None

            else:
                if abs(resValue) > scientificNotationThreshold:
                    resValue = str(resValue)

                elif allResultDigits is False: # Digits based on error
                    resError = r.get('resultError')
                    resRepr = dataClass.valueErrorRepr(resValue, resError, valueDecimalsWithNoneError=2, printErrorPart=False)
                    # resValue = float(resRepr)
                    resValue = resRepr # string
                else:
                    resValue = str(resValue)
            
            acronym = '_'.join([resName, loc]+reqTags)
            rowDict[acronym] = resValue

        # Execution dates
        rowDict['earliestTestDate'] = earliestTestDate
        rowDict['latestTestDate'] = latestTestDate

    # dataFrame = DataFrame.from_dict({k: [v] for k, v in rowDict.items()})
    # dataFrame = DataFrame(rowDict, index=[0])
        
    # print(f'DEBUG: rowDict generated ({rowDict})')
    
    componentDF = emptyDotOutDataFrame.copy()

    # for key, val in rowDict.items():
    #     if key in componentDF.columns:
    #         componentDF[key] = val

    componentDF.iloc[-1] = rowDict


    # return concat([
    #         emptyDotOutDataFrame,
    #         dataFrame
    #     ],
    #     ignore_index=True)
            
    return componentDF

# ------------------------------------------------------------------------------
# Grouped components dot out dataframe generation functions (deprecated)


def _retrieveComponentGroupBlueprints(connection, componentGroup:list,
                                *,
                                verbose:bool = True):
    """Given a group of components, this method queries the database and
    returns all the blueprints associated to them.

    Args:
        connection (mom.connection): The connection object to the MongoDB
            server.
        componentGroup (list[mom.component]): The list of components of which
            the blueprints have to be retrieved.

    Keyword Args:
        verbose (bool, optional): If False, query output is suppressed.
            Defaults to True.

    Returns:
        List[mom.blueprint] | None: The list of blueprints retrieved.
    """        

    # Selecting only component instances (None is excluded)
    group = [cmp for cmp in componentGroup if isinstance(cmp, mom.component)]
    bpIDs = [cmp.getField('blueprintID', verbose = False) for cmp in group]
    bpIDs = [mom.toObjectID(ID) for ID in bpIDs if ID is not None] # Removing None
    bpIDs = list(set(bpIDs)) # Removing duplicates

    bps = mom.query(connection, qu.among('_id', bpIDs), None,
                    mom.blueprint.defaultDatabase,
                    mom.blueprint.defaultCollection,
                    returnType = 'native', verbose = verbose)

    if bps is None:
        return None

    else:
        bps = [bp for bp in bps if bp is not None]
    
    if bps == []:
        return None
    
    return bps


def _groupedDotOutDataFrame(components:list, blueprints:list = None,
                           connection:mom.connection = None,
                           *,
                           allResultDigits:bool = False,
                           scientificNotationThreshold:float = 10**9,
                           ) -> DataFrame:
    """Generates a DotOut dataframe for a group of components.

    Args:
        components (list[mom.components]): The group of components for which
            the dot-out table has to be generated.
        blueprints (list[mom.blueprints], optional): The list of blueprints
            associated to the components. If not passed, the blueprints are
            queried from the database, in which case the connection argument
            has to be passed as well.
        connection(mom.connection, optional): The mongomanager.connection
            object to the MongoDB database. Must be passed if blueprints is not
            None. Defaults to None.

    Keyword Args:
        allResultDigits (bool): If True, all the digits for results are reported,
            otherwise the numbers are rounded. Defaults to False.
        scientificNotationThreshold (float, optional): The threshold for the
            absolute value of numbers over which they are reported in scientific
            notation. Defaults to 10**9.s

    Returns:
        DataFrame: The dot-out DataFrame for the components.
    """    
    
    log.debug(f'[groupedDotOutDataFrame] no. components: {len(components)}')

    # I need the blueprints to generate the acronyms
    if blueprints is None:
        blueprints = _retrieveComponentGroupBlueprints(connection, components)
    
    # I generate the acronyms strings to be used as columns of the dot-out table
    if blueprints is None:
        acronyms = None
    else:
        acronyms = []
        for bp in blueprints:
            bpAcronyms = acronymsFromBlueprint(bp)
            newAcronyms = [acr for acr in bpAcronyms if acr not in acronyms]
            acronyms.extend(newAcronyms)

    log.debug(f'[groupedDotOutDataFrame] no. acronyms: {len(acronyms)}')

    # I generate and concatenate the dot-out table for all the components
    DFs = [dotOutDataFrame(
        emptyDotOutDataFrame = _spawnEmptyDotOutDataframe(acronyms),
        component = cmp,
        componentDotOutData = None, # Generated from component
        allResultDigits = allResultDigits,
        scientificNotationThreshold = scientificNotationThreshold,
    ) for cmp in components]
    
    DFs = [DF for DF in DFs if DF is not None]
    log.debug(f'[groupedDotOutDataFrame] no. non-empty: {len(DFs)}')
    if DFs == []: return None

    log.debug(f'[groupedDotOutDataFrame] DFs: {DFs}')
    groupedDataFrame = concat(DFs)

    return groupedDataFrame


def _moduleBatchDotOutDataFrame(connection, batch:str, * ,
                                    allResultDigits:bool = False,
                                    scientificNotationThreshold:float = 10**9,
                                    ) -> DataFrame:
    """Returns the dot-out dataframe for a batch of modules.

    Args:
        connection (mom.connection): The connection to the MongoDB database.
        batch (str): The string identifying the batch of modules.
    
    Keyword Args:
        allResultDigits (bool): If True, all the digits for results are reported,
            otherwise the numbers are rounded. Defaults to False.
        scientificNotationThreshold (float, optional): The threshold for the
            absolute value of numbers over which they are reported in scientific
            notation. Defaults to 10**9.s

    Returns:
        DataFrame: The dot-out DataFrame for the module batch.
    """
    
    # Type checks
    if not isinstance(batch, str):
        raise TypeError('"batch" must be a string.')
    
    b = morm.moduleBatch(connection, batch)
    if b is None:
        log.warning(f'No batch can be found from string "{batch}".')
        return None

    mods = b.modules
    bps = b.moduleBPs

    dataFrame = _groupedDotOutDataFrame(mods, bps,
                                       allResultDigits = allResultDigits,
                                       scientificNotationThreshold = scientificNotationThreshold)
    if dataFrame is None: return None

    # Adding information relative to the batch

    # Prepending the batch column
    dataFrame.insert(0, "batch", len(dataFrame)*[batch])

    # Renaming some columns
    dataFrame.rename(columns={"component": "module",
                              "componentID": "moduleID"}, inplace=True)

    return dataFrame


def _waferCollationDotOutDataFrame(connection, waferName:str, *,
                                  allResultDigits:bool = False,
                                  scientificNotationThreshold:float = 10**9,
                                  ) -> DataFrame:
    """Returns the dot-out dataframe for a wafer collation.

    Args:
        connection (mom.connection): The connection to the MongoDB database.
        waferName (str): The name of the wafer for which the chips are retrieved.
    
    Keyword Args:
        allResultDigits (bool): If True, all the digits for results are reported,
            otherwise the numbers are rounded. Defaults to False.
        scientificNotationThreshold (float, optional): The threshold for the
            absolute value of numbers over which they are reported in scientific
            notation. Defaults to 10**9.s

    Returns:
        DataFrame: The dot-out DataFrame for the module batch.
    """

    # Type checks
    if not isinstance(waferName, str):
        raise TypeError('"waferName" must be a string.')

    wc = morw.waferCollation(connection, waferName)
    if wc is None:
        log.warning(f'No wafer collation can be found from wafer name "{waferName}".')


    # Early exit
    if wc.chipBlueprints is None:
        log.warning(f'No chip blueprints associated to wafer "{wc.wafer.name}".')
        return None
    

    dataFrame = _groupedDotOutDataFrame(wc.chips, wc.chipBlueprints,
                                       allResultDigits = allResultDigits,
                                       scientificNotationThreshold = scientificNotationThreshold)

    # Adding information relative to the batch

    # Prepending the batch column
    dataFrame.insert(0, "waferID", len(dataFrame)*[wc.wafer.ID])
    dataFrame.insert(0, "wafer", len(dataFrame)*[waferName])

    dataFrame.rename(columns={"component": "chip",
                              "componentID": "chipID"}, inplace=True)

    return dataFrame


# ------------------------------------------------------------------------------
# Pandas DataFrame utilities

def _prependConstantColumn(DF:DataFrame, columnName:str, columnValue) -> None:
    DF.insert(0, columnName, len(DF)*[columnValue])

def _appendConstantColumn(DF:DataFrame, columnName:str, columnValue) -> None:
    DF.insert(len(DF.columns), columnName, len(DF)*[columnValue])

def _insertConstantColumn(DF:DataFrame, columnName:str, index:int, columnValue) -> None:
    DF.insert(index, len(DF.columns), columnName, len(DF)*[columnValue])

def _insertColumn(DF:DataFrame, columnName:str, index:int, columnValues:list) -> None:
    DF.insert(index, len(DF.columns), columnName, columnValues)

def _renameColumns(DF:DataFrame, renamingMap:dict) -> None:
    DF.rename(columns = renamingMap, inplace = True)

def _deleteColumns(DF:DataFrame, columnNames:list[str]) -> None:
    DF.drop(columns = columnNames, inplace = True)

def _replaceColumnValues(DF:DataFrame, columnName:str, newValues:list) -> None:
    DF[columnName] = newValues

def _mapFunctionToColumn(DF:DataFrame, columnName:str, function:callable) -> None:

    columnValues = list(DF[columnName])
    newValues = [function(val) for val in columnValues]
    _replaceColumnValues(DF, columnName, newValues)


# ------------------------------------------------------------------------------
# Single component dot out dataframe generation functions

# Abstract
def _singleComponentDotOutDataFrame(connection, component, *,
                                allResultDigits:bool = False,
                                scientificNotationThreshold:float = 10**9,
                            ):
    """Returns the dot-out dataframe for a given compoent.

    This is an abstract function, returning a dot-out table with column names
    based on fields following the R&D conventions."""
    
    bp = component.retrieveBlueprint(connection)
    if bp is None:
        raise Exception(f'Could not generate dot out dataframe because component "{component.name}" has no associated blueprint.')
    
    acronyms = acronymsFromBlueprint(bp)
    emptyDF = _spawnEmptyDotOutDataframe(acronyms)

    dotOutData = retrieveComponentDotOutData(component)
    if dotOutData is None:
        log.warning(f'No Dot Out data available for component "{component.name}".')
        return None

    DF = dotOutDataFrame(emptyDF, component, dotOutData,
                         allResultDigits=allResultDigits,
                         scientificNotationThreshold=scientificNotationThreshold)
    
    return DF


def _singleChipDotOutDataFrame(connection, component, *,
                            allResultDigits:bool = False,
                            scientificNotationThreshold:float = 10**9,
                            waferName:str):
    """Returns the dot-out dataframe for a given optical chip.
    
    It also prepends the wafer name to the table."""

    DF = _singleComponentDotOutDataFrame(connection, component,
        allResultDigits=allResultDigits,
        scientificNotationThreshold=scientificNotationThreshold)
    if DF is None: return None

    _prependConstantColumn(DF, 'waferName', waferName)

    return DF


def _singleModuleDotOutDataFrame(connection, component, *,
                            allResultDigits:bool = False,
                            scientificNotationThreshold:float = 10**9,
                            batchCode:str):
    """Returns the dot-out dataframe for a given module.
    
    It also prepends the module batch to the table."""
    
    DF = _singleComponentDotOutDataFrame(connection, component,
        allResultDigits=allResultDigits,
        scientificNotationThreshold=scientificNotationThreshold)
    if DF is None: return None

    _prependConstantColumn(DF, 'batch', batchCode)

    return DF


# ==============================================================================
# Dot Out file management

class DotOutManagementException(Exception):
    pass

class DotOutFileManager:
    """This class is used to manage the .out files.
    
    It is used to create the files, and to append a new line to them starting
    from a dot-out dataframe for a single component."""
    
    @staticmethod
    def _extractHeaderData(DF:DataFrame):

        csvString = DF.to_csv(index = False)

        lines = csvString.split('\n')
        header = lines[0]
        data = lines[1]

        return header, data
    
    @staticmethod
    def _writeLine(filePath:Path, line:str):

        with open(filePath, 'a') as fileOut:
            fileOut.write(line)

    @classmethod
    def _createDotOutFile(cls, filePath:Path, header:str):

        if filePath.exists():
            raise DotOutManagementException(f'File already exists. Cannot create. ({filePath}).')
        
        cls._writeLine(filePath, header)
        print('(Written Header)')

    @classmethod
    def appendData(cls, filePath:Path, singleComponentDotOutDF:DataFrame):
        """This method appends a new line to the .out file starting from the
        dot-out dataframe for a single component."""

        try:
            log.important(f'Appending data to out file ({filePath}).')

            header, data = cls._extractHeaderData(singleComponentDotOutDF)

            if not filePath.exists():
                log.important(f'File does not exist. Creating it.')
                cls._createDotOutFile(filePath, header)

            cls._writeLine(filePath, data)
            log.important(f'New line written.')

        except Exception as e:
            log.error(f'[DotOutFileManager] Something went wrong when writing data to file ({filePath}).')
            log.error(e)
            return None
        
# ==============================================================================
# Support functions

def chipID(chipName):
    """Valid for all chip types, including Cordoba."""

    if 'COR' in chipName:
        return chipID_cordoba(chipName)

    waferName, chipSerial = chipName.split('_')

    ID = chipSerial.split('-')[-1] # Should be last two digits

    assert int(ID) <= 99
    assert int(ID) >= 0

    return ID

def chipID_cordoba(chipName):
    """Chip name is in the form "3CAxxxx_COR-yy-zz" and has to be
    brought to "3CAxxxxww", where ww depends on yy and zz:
    
    rules:
        COR-V1-01 -> 01 (V1 -> +0)
        COR-V2-01 -> 31 (V2 -> +30)
        COR-V3-01 -> 46 (V1 -> +45)
    """

    waferName, chipSerial = chipName.split('_')

    _, V, N = chipSerial.split('-')
    N = int(N)
    Vnum = int(V[1])

    if Vnum == 1:
        newN = N
    elif Vnum == 2:
        newN = N + 30
    elif Vnum == 3:
        newN = N + 45

    chipID = f'{newN:02}'
    return chipID

def DUT_ID(chipName:str):

    waferName, _ = chipName.split('_', maxsplit = 1)
    return waferName + chipID(chipName)
    

# =============================================================================
# Global manager


class DotOutManager(ABC):
    """This class is used to manage the generation of dot-out tables for
    components and for appending the corresponding data to the .out files."""

    def __init__(self, connection,
            folderPath:Path,
            mongoDBupload:bool = False,
        ):
        self.connection = connection
        self.folderPath = folderPath
        self.mongoDBupload = mongoDBupload
    
    def _generateDotOutData(self, component):
        generateComponentDotOutData(component, self.connection)

    def _generateAndSaveDotOutData(self, component):
        generateAndSaveComponentDotOutData(component, self.connection)


    @abstractmethod
    def _dotOutDF(self, component):
        pass

    def __dotOutDF(self, component):

        try:
            DF = self._dotOutDF(component)

            if DF is None: return None
            if not isinstance(DF, DataFrame):
                raise DotOutManagementException(f'The _dotOutDF method of class {self.__class__.__name__} has not returned a DataFrame or None (it returned {type(DF)}).')
            
            return DF

        except Exception as e:
            log.error(f'[DotOutManager] Something went wrong during the generation of the dot out dataframe for component "{component.name}".')
            log.error(e)
            return None

    @abstractmethod
    def _dotOutFilePath(self, component) -> Path:
        pass

    def __dotOutFilePath(self, component) -> Path:

        try:
            filePath = self._dotOutFilePath(component)

            if not isinstance(filePath, Path):
                raise DotOutManagementException(f'The _dotOutFilePath method of class {self.__class__.__name__} has not returned a pathlib.Path object (it returned {type(filePath)}).')

            return filePath
        
        except Exception as e:
            log.error(f'[DotOutManager] Something went wrong when determing the .out file path for component "{component.name}".')
            log.error(e)
            return None


    def saveDotOutLine(self, component):
        
        log.important(f'Started saving dot out line for component "{component.name}".')

        if self.mongoDBupload is True:
            log.important(f'mongoDBupload = True: Generating datasheet for component and uploading to MongoDB.')

            try:
                self._generateAndSaveDotOutData(component)
            except Exception as e:
                log.error('[DotOutManager.saveDotOutLine] Something went wrong when generating and uploading datasheet data to MongoDB.')
                log.error(e)
                return None
        
        else:
            log.important(f'mongoDBupload = False: Generating datasheet for component (without uploading to MongoDB).')
            
            try:
                self._generateDotOutData(component)
            except Exception as e:
                log.error('[DotOutManager.saveDotOutLine] Something went wrong when generating datasheet data to MongoDB.')
                log.error(e)
                return None
        
        log.important(f'Generating dot out dataframe.')

        DF = self.__dotOutDF(component)
        if DF is None: return None

        filePath = self.__dotOutFilePath(component)
        if filePath is None: return None

        DotOutFileManager.appendData(filePath, DF)


class DotOutManager_Modules(DotOutManager):

    def __init__(self, connection, folderPath:Path,
                 mongoDBupload:bool = False,
                 allResultDigits:bool = False,
                 scientificNotationThreshold:float = 10**9,
                 ):
        super().__init__(connection, folderPath, mongoDBupload)
        self.allResultDigits = allResultDigits
        self.scientificNotationThreshold = scientificNotationThreshold

    def _dotOutFilePath(self, module) -> Path:
        
        batch = module.getField('batch', verbose = False)
        fileName = batch.replace('/', '-') + '.out'

        filePath = self.folderPath / fileName
        return filePath

    def _dotOutDF(self, module) -> DataFrame:
        
        DF = _singleModuleDotOutDataFrame(self.connection, module,
                allResultDigits = self.allResultDigits,
                scientificNotationThreshold = self.scientificNotationThreshold,
                batchCode = module.getField('batch', verbose = False))
        if DF is None: return None

        _deleteColumns(DF, ['componentID'])
        _renameColumns(DF,
            {
                'batch': 'LOT_ID',
                'component': 'ChipID',
            }
        )

        return DF


class DotOutManager_Chips(DotOutManager):

    def __init__(self, connection, folderPath:Path,
                 mongoDBupload:bool = False,
                 allResultDigits:bool = False,
                 scientificNotationThreshold:float = 10**9,
                 ):
        super().__init__(connection, folderPath, mongoDBupload)
        self.allResultDigits = allResultDigits
        self.scientificNotationThreshold = scientificNotationThreshold


    def _dotOutFilePath(self, chip) -> Path:
        
        waferName = chip.name.split('_', maxsplit = 1)[0]
        fileName = waferName + '.out'

        filePath = self.folderPath / fileName
        return filePath

    def _dotOutDF(self, chip) -> DataFrame:

        wafer = chip.ParentComponent.retrieveElement(self.connection)
        chipName = chip.name

        DF = _singleChipDotOutDataFrame(self.connection, chip,
                allResultDigits = self.allResultDigits,
                scientificNotationThreshold = self.scientificNotationThreshold,
                waferName = wafer.name)
        if DF is None: return None

        _prependConstantColumn(DF, 'DUT_ID', DUT_ID(chipName))
        _mapFunctionToColumn(DF, 'component', chipID)
        _deleteColumns(DF, ['componentID'])
        _renameColumns(DF,
            {
                'waferName': 'LOT_ID',
                'component': 'ChipID',
            }
        )

        return DF