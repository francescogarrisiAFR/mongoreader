from mongoutils import queryUtils as qu
import mongoreader.wafers as morw
import mongoreader.modules as morm
from .MMSconnectors_benchConfig import benchConfig
from .conversions import Converter_dotOutChipID, Converter_dotOutDUTID
from datautils import dataClass
import mongomanager as mom
from mongomanager.errors import FieldNotFound
from mongomanager import log
from pandas import DataFrame, concat, isnull
from typing import TypedDict
from pathlib import Path
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import wraps
from subprocess import run, CompletedProcess, CalledProcessError, TimeoutExpired
from traceback import format_exc
from socket import gethostname
import re


# ------------------------------------------------------------------------------
# Constants and settings

GENERAL_FIELDS_MONGODB = ['component', 'componentID',
        'earliestTestDate', 'latestTestDate', 'bench', 'operator']

# Process stage acronyms
# N.B. These are currently (6/3/2024) used only for Cordoba chips.
OLDSTAGES_MONGODB = ['Chip testing', 'Final test']
STAGES_MMS = ['VFBCFIN', 'BCBCFIN']
STAGES_MONGODB_TO_MMS = dict(zip(OLDSTAGES_MONGODB, STAGES_MMS))

# Part numbers

PART_NUMBERS = { # <Wafer two-letter acronym>: <Part Number>
    'CA': 'PA015945', # Cordoba
}

# Chip IDs

# Cordoba M version
CORDOBA_M1_SERIALS_TO_CHIP_IDS = {
    'A00-COR-M1': 0,
    'A01-COR-M1': 1,
    'A02-COR-M1': 2,
    'A03-COR-M1': 3,
    'A04-COR-M1': 4,
    'A05-COR-M1': 5,
    'A06-COR-M1': 6,
    'A07-COR-M1': 7,
    'B00-COR-M1': 8,
    'B01-COR-M1': 9,
    'B02-COR-M1': 10,
    'B03-COR-M1': 11,
    'B04-COR-M1': 12,
    'B05-COR-M1': 13,
    'B06-COR-M1': 14,
    'B07-COR-M1': 15,
    'B08-COR-M1': 16,
    'B09-COR-M1': 17,
    'C00-COR-M1': 18,
    'C01-COR-M1': 19,
    'C02-COR-M1': 20,
    'C03-COR-M1': 21,
    'C04-COR-M1': 22,
    'C05-COR-M1': 23,
    'C06-COR-M1': 24,
    'C07-COR-M1': 25,
    'C08-COR-M1': 26,
    'C09-COR-M1': 27,
    'D00-COR-M1': 28,
    'D01-COR-M1': 29,
    'D02-COR-M1': 30,
    'D03-COR-M1': 31,
    'D04-COR-M1': 32,
    'D05-COR-M1': 33,
    'D06-COR-M1': 34,
    'D07-COR-M1': 35,
    'D08-COR-M1': 36,
    'D09-COR-M1': 37,
    'E00-COR-M1': 38,
    'E01-COR-M1': 39,
    'E02-COR-M1': 40,
    'E03-COR-M1': 41,
    'E04-COR-M1': 42,
    'E05-COR-M1': 43,
    'E06-COR-M1': 44,
    'E07-COR-M1': 45,
    'E08-COR-M1': 46,
    'E09-COR-M1': 47,
    'F00-COR-M1': 48,
    'F01-COR-M1': 49,
    'F02-COR-M1': 50,
    'F03-COR-M1': 51,
    'F04-COR-M1': 52,
    'F05-COR-M1': 53,
    'F06-COR-M1': 54,
    'F07-COR-M1': 55
}

DOT_OUT_LOG_FOLDER = Path(r"T:\Projects\BE_folder\Software automation\Dot Out Generation\Dot Out Generation Logs")

# ------------------------------------------------------------------------------
# Exceptions

class DotOutException(Exception):
    """Base class for exceptions raised during the generation of the dot-out
    tables."""

    pass

class FailedDatasheetGeneration(DotOutException):
    """Raised when the generation of the datasheet data fails."""

    pass

class MissingBenchConfigException(DotOutException):
    """Raised when a bench configuration is not found."""
    
    pass


# ------------------------------------------------------------------------------
# Decorators

def deprecatedFunction(function):
    """If a function is a @deprecatedFunction, a deprecation warning is issued
    when the function is called, and the documentation is updated by prepending
    the string "[This method is deprecated and documentation is not updated.]".
    """

    deprMessage = "[This function is deprecated and documentation is not updated.]"

    docs = function.__doc__

    if docs is None:
        function.__doc__ = deprMessage
    else:
        function.__doc__ = deprMessage + "\n\n" + docs

    @wraps(function)
    def wrapper(*args, **kwargs):
        log.warning(f'Warning! The function "{function.__name__}" is deprecated.')
        return function(*args, **kwargs)
    

# ------------------------------------------------------------------------------

def _isDateNone(date):
    """Assuming date is either None, a datetime object or a pandas.NaT object,
    this method returns True if date is None or pandas.NaT, False otherwise."""
    
    if date is None: return True
    if isnull(date): return True

    return False

def _isNumberNone(number):
    """Assuming date is either None, a datetime object or a pandas.NaT object,
    this method returns True if date is None or pandas.NaT, False otherwise."""
    
    if number is None: return True
    if isnull(number): return True

    return False


def _logError(e):

    log.error(e)
    try:
        log.error(f'{format_exc(e.__traceback__)}')
    except:
        log.error(f'Could not log traceback for error {e}.')
    

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

        reqTags = None
        if 'tagFilters' in entry:
            reqTags = entry['tagFilters'].get('required')
            if reqTags == []: reqTags = None

        if reqTags is None:
            log.warning(f'No required tags for result "{resName}" at location group "{entry["locationGroup"]}". Associated acronyms are generated without trailing tags.')

        for loc in locations:
            
            toJoin = [resName, loc]
            if reqTags is not None: toJoin.extend(reqTags)

            acronym = '_'.join(toJoin)
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
        GENERAL_FIELDS_MONGODB + acronyms)


def generateComponentDotOutData(component, connection, blueprint,
                                processStage_orStages:str|list[str] = None) -> None:
    """Uses the datasheet definition found in a component's blueprint to
    generate a new datasheet and store it in the component.
    
    The data is NOT uploaded to the database."""

    log.debug(f'[generateComponentDotOutData] component name: {component.name}')
    log.debug(f'[generateComponentDotOutData] connection: {connection}')
    log.debug(f'[generateComponentDotOutData] blueprint name: {blueprint.name if blueprint is not None else "<No blueprint>"}')
    log.debug(f'[generateComponentDotOutData] processStage_orStages: {processStage_orStages}')

    if connection is None and blueprint is None:
        raise ValueError(f'"connection" and "blueprint" cannot be both None when generating dot-out data.')
    
    if connection is not None and blueprint is not None:
        raise ValueError(f'"connection" and "blueprint" cannot be both not None when generating dot-out data.')

    if blueprint is not None:
        if not isinstance(blueprint, mom.blueprint):
            raise TypeError(f'When not None, "blueprint" must be a mongomanager.blueprint object (it is {type(blueprint)}).')

    if processStage_orStages is None:
        stages = None

    elif isinstance(processStage_orStages, list):
        if not all([isinstance(stage, str) for stage in processStage_orStages]):
            raise TypeError(f'If it is a list, all elements in "processStage_orStages" must be strings.')
        stages = processStage_orStages
        log.debug(f'[generateComponentDotOutData] multiple stages: {stages}')

    elif isinstance(processStage_orStages, str):
        stages = [processStage_orStages]
    else:
        raise TypeError(f'"processStage_orStages" must be a string, a list of strings, or None (it is {type(processStage_orStages)}).')

    if connection is not None:
        DS = component.Datasheet.createAndStoreDatasheet(connection,
                                requiredProcessStages = stages,
                                requiredStati = None)
    
    if blueprint is not None:
        log.warning('generateComponentDotOutData is generating the datasheet using the provided blueprint. This mode is meant to be used only for debugging.')
        
        DSD = blueprint.getDatasheetDefinition()
        locationsDict = blueprint.Locations.retrieveGroupsDict()

        if DSD is None:
            raise Exception(f'I cannot proceed to generate the datasheet because the blueprint "{blueprint.name}" has no datasheet definition.')
        if locationsDict is None:
            raise Exception(f'I cannot proceed to generate the datasheet because the blueprint "{blueprint.name}" has no locations.')

        DS = component.Datasheet.createAndStoreDatasheet(connection,
                requiredProcessStages = stages,
                requiredStati = None,
                datasheetDefinition = DSD,
                locationsDict = locationsDict)
        
    if DS is None:
        raise FailedDatasheetGeneration(f'Failed to generate datasheet for component "{component.name}".')




def generateAndSaveComponentDotOutData(component, connection, blueprint,
                                       processStage_orStages:str|list[str] = None) -> None:
    """Uses the datasheet definition found in a component's blueprint to
    generate a new datasheet and store it in the component.

    The data is then uploaded to the database (the component is mongoReplaced).
    """

    if connection is None:
        raise TypeError(f'"cannection" cannot be None when generating and saving dot-out data.')

    if blueprint is not None:
        # Generating data from datasheet definition stored in the blueprint
        generateComponentDotOutData(component, None, blueprint,
                                processStage = processStage_orStages)
    else:
        # The blueprint is automatically retrieved from the component
        generateComponentDotOutData(component, connection, None,
                                processStage = processStage_orStages)
        
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
        'earliestTestDate': None, # To be set later
        'latestTestDate': None, # To be set later
        'bench': None, # To be set later
        'operator': None, # To be set later
    }

    # Populating the acronym fields
    if componentDotOutData is not None:
        
        componentDotOutData = componentDotOutData.reset_index()  # make sure indexes pair with number of rows

        earliestTestDate = None
        latestTestDate = None

        for index, r in componentDotOutData.iterrows():

            # Execution date

            date = r.get('executionDate')

            log.debug(f'[dotOutDataFrame] [{index:3}] date: {date}')
            
            if not _isDateNone(date):

                if earliestTestDate is None:
                    log.debug(f'[dotOutDataFrame] [{index:3}] Assigning earliest date for the first time ({date}).')
                    earliestTestDate = date

                if latestTestDate is None:
                    log.debug(f'[dotOutDataFrame] [{index:3}] Assigning latest date for the first time ({date}).')
                    latestTestDate = date

                if date < earliestTestDate:
                    earliestTestDate = date
                    log.debug(f'[dotOutDataFrame] [{index:3}] Found an earlier date ({date}).')
                if date > latestTestDate:
                    latestTestDate = date
                    log.debug(f'[dotOutDataFrame] [{index:3}] Found a later date ({date}).')

            # Result acronym and value

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

                    if _isNumberNone(resValue): resValue = None
                    if _isNumberNone(resError): resError = None

                    resRepr = dataClass.valueErrorRepr(resValue, resError, valueDecimalsWithNoneError=2, printErrorPart=False)
                    # resValue = float(resRepr)
                    resValue = resRepr # string
                else:
                    resValue = str(resValue)
            
            if reqTags is None: reqTags = []
            acronym = '_'.join([resName, loc]+reqTags)
            rowDict[acronym] = resValue

            # Bench and operator

            # N.B. The code below collects the bench and operator from the last
            # result entry scooped from the datasheet data. This masks the
            # situation where the bench and operator are different for different
            # testReports. This is a limitation of the current implementation.

            bench = r.get('bench')
            rowDict['bench'] = bench

            operator = r.get('operator')
            rowDict['operator'] = operator

        # Execution dates
        rowDict['earliestTestDate'] = earliestTestDate
        rowDict['latestTestDate'] = latestTestDate

    # dataFrame = DataFrame.from_dict({k: [v] for k, v in rowDict.items()})
    # dataFrame = DataFrame(rowDict, index=[0])
        
    # print(f'DEBUG: rowDict generated ({rowDict})')
    
    componentDF = emptyDotOutDataFrame.copy()
    componentDF.iloc[-1] = rowDict


    # return concat([
    #         emptyDotOutDataFrame,
    #         dataFrame
    #     ],
    #     ignore_index=True)
            
    return componentDF

# ------------------------------------------------------------------------------
# Grouped components dot out dataframe generation functions (deprecated)

@deprecatedFunction
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

@deprecatedFunction
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

@deprecatedFunction
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

@deprecatedFunction
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

# These functions implement operations on pandas DataFrames that are used to
# generate the dot-out tables.

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

# Abstract function to be implemented for each component type
def _singleComponentDotOutDataFrame(connection, component, *,
                                blueprint:mom.blueprint = None,
                                allResultDigits:bool = False,
                                scientificNotationThreshold:float = 10**9,
                            ):
    """Returns the dot-out dataframe for a given compoent.

    This is an abstract function, returning a dot-out table with column names
    based on fields following the R&D conventions.
    
    The datasheet data in the component are not generated by this function."""
    
    
    if blueprint is None:
        # with mom.recallDocuments():
        bp = component.retrieveBlueprint(connection)
    else:
        bp = blueprint

    if bp is None:
        raise Exception(f'Could not generate dot out dataframe because component "{component.name}" has no associated blueprint.')
    if not isinstance(bp, mom.blueprint):
        raise TypeError(f'The retrieved blueprint is not actually a mongomanager.blueprint (it is {type(bp)}).')
    
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
                            blueprint:mom.blueprint = None,
                            allResultDigits:bool = False,
                            scientificNotationThreshold:float = 10**9,
                            waferName:str):
    """Returns the dot-out dataframe for a given optical chip.
    
    It also prepends the wafer name to the table.
    
    The datasheet data in the component are not generated by this function"""

    DF = _singleComponentDotOutDataFrame(connection, component,
        blueprint=blueprint,
        allResultDigits=allResultDigits,
        scientificNotationThreshold=scientificNotationThreshold)
    if DF is None: return None

    _prependConstantColumn(DF, 'waferName', waferName)

    return DF


def _singleModuleDotOutDataFrame_legacy(connection, component, *,
                            blueprint:mom.blueprint = None,
                            allResultDigits:bool = False,
                            scientificNotationThreshold:float = 10**9,
                            batchCode:str):
    """Returns the dot-out dataframe for a given module.
    
    It also prepends the module batch to the table.

    The datasheet data in the component are not generated by this function."""
    
    DF = _singleComponentDotOutDataFrame(connection, component,
        blueprint=blueprint,
        allResultDigits=allResultDigits,
        scientificNotationThreshold=scientificNotationThreshold)
    if DF is None: return None

    _prependConstantColumn(DF, 'batch', batchCode)

    _deleteColumns(DF, ['componentID'])
    _renameColumns(DF,
        {
            # 'batch': 'LOT_ID',
            'component': 'Module_Name',
        }
    )
    _prependConstantColumn(DF, 'Package_ID', modulePackageID(component))

    return DF

def _singleModuleDotOutDataFrame_CDM128(connection, component, *,
                            blueprint:mom.blueprint = None,
                            allResultDigits:bool = False,
                            scientificNotationThreshold:float = 10**9):
    """Returns the dot-out dataframe for a given CDM128 module.
    
    It also prepends the module batch to the table.

    The datasheet data in the component are not generated by this function."""
    
    DF = _singleComponentDotOutDataFrame(connection, component,
        blueprint=blueprint,
        allResultDigits=allResultDigits,
        scientificNotationThreshold=scientificNotationThreshold)
    if DF is None: return None

    _renameColumns(DF, {
        'component': 'Module_Name',
        'componentID': 'MongoDB_ID',
    })

    # Searching for supportDictionary.MMSrecord

    MMSrecord = component.getField(['supportDictionary', 'MMSrecord', 'MMSinfo'], verbose = False)
    if MMSrecord is None:
        pass
        log.error(f'No MMS record found for component "{component.name}" (ID: "{component.ID}").')

    for key, val in reversed(MMSrecord.items()):
        _prependConstantColumn(DF, key, val)

    return DF


# ==============================================================================
# Dot Out file management

class DotOutManagementException(Exception):
    """Base class for exceptions raised during the generation of the dot-out
    tables."""

class DotOutFileManager:
    """This class is used to manage the writing of .out files.
    
    It is used to create the files, and to append a new line to them starting
    from a dot-out dataframe for a single component.
    
    The dot out table is generated using the DotOutManager class, and then
    written to the file using this class."""
    
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
    def _writeHeader(cls, filePath:Path, header:str):

        cls._writeLine(filePath, header)
        cls._writeLine(filePath, '\n') # Add additional empty line before data
        mom.log.spare('(Written Header)')

    @classmethod
    def _createDotOutFile(cls, filePath:Path, header:str):

        if filePath.exists():
            raise DotOutManagementException(f'File already exists. Cannot create. ({filePath}).')
        
        cls._writeHeader(filePath, header)

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
            _logError(e)
            return None
        
# ==============================================================================
# Support functions

def hostname() -> str:
    return gethostname().upper()

def getBenchConfig(hostname:str) -> dict:

    for config in benchConfig:
        if config['hostname'] == hostname:
            return config
    
    raise MissingBenchConfigException(f'No bench configuration found for hostname "{hostname}".')

def chipID(chipName:str) -> str:
    return Converter_dotOutChipID.chipID_fromMongoName(chipName)

def DUT_ID(chipName:str):
    return Converter_dotOutDUTID.DUT_ID_fromMongoName(chipName)

def LOT_ID(chipName:str):
    """E.g. "3CAxxxx_COR-V1-01" -> "3CAxxxx"."""

    waferName, _ = chipName.split('_', maxsplit = 1)
    return waferName


def chipType(chipName:str):
    """E.g.
    chipName: "3CAxxxx_COR-V1-01" -> "COR-V1".
    chipName: "3CAxxxx_E08-COR-M1" -> "COR-M1".
    chipName: "3DR0001_DR8-01" -> "DR8".
    chipName: "2BI0016_05-SE -> "SE"    
    """

    if 'COR' in chipName:
        
        _, chipSerial = chipName.split('_')

        if chipSerial in CORDOBA_M1_SERIALS_TO_CHIP_IDS:
            return chipSerial.split('-', maxsplit=1)[1]

        serialParts = chipSerial.split('-')
        chipType = serialParts[0] + '-' + serialParts[1] 

    elif 'DR' in chipName:
        serial = chipName.split('_')[1]
        chipType = serial.rsplit('-', maxsplit = 1)[0]

    else:
        raise NotImplementedError(f'chipType() not implemented for chip "{chipName}".')

    return chipType
    

# =============================================================================
# Global manager


class DotOutManager(ABC):
    """This class is used to manage the generation of dot-out tables for
    components and for appending the corresponding data to the .out files.
    
    The class is abstract and has to be subclassed for each component type.
    In particular, the subclass has to implement the _dotOutDF and
    _dotOutFilePath methods, which return the dot out table for that component
    and the file path where the data should be saved.
    """

    def __init__(self, connection,
            folderPath:Path = None,
            blueprint:mom.blueprint = None,
            processStage:str = None,
            mongoDBupload:bool = False,
            MMSupload:bool = True,
            Out2EDCpath:Path = None,
        ):
        """Constructor method (__init__) of DotOutManager.

        Args:
            connection (mom.connection): The connection to the MongoDB database.
            folderPath(pathlib.Path, optional): The path to the folder where the
                .out files are saved. If not passed, the default path for the
                bench is retrieved from MMSconnector_benchConfig.
            blueprint (mom.blueprint, optional): If passed, the datasheet
                definition is retrieved from the blueprint. Meant for testing
                purposes. Defaults to None.
            processStage (str, optional): The process stage for which the data
                is generated. If None, the data is generated for all process
                stages. Defaults to None.
            mongoDBupload (bool, optional): If True, the generated data (the
                component datasheet) is uploaded to the database. Defaults to
                False.
            MMSupload (bool, optional): If True, the line appended to the .out
                file is also uploaded to the MMS database using the Out2EDC
                executable. Defaults to True.
            Out2EDCpath (pathlib.Path, optional): The path to the local 
                Out2EDC.exe executable. If not passed, the default path for the
                bench is retrieved from MMSconnector_benchConfig. Defaults to
                None.
        """

        log.debug('[DotOutManager.__init__] DotOutManager initialized.')
        log.debug(f'[DotOutManager.__init__] connection: {connection}')
        log.debug(f'[DotOutManager.__init__] folderPath: {folderPath}')
        log.debug(f'[DotOutManager.__init__] blueprint: {blueprint}')
        log.debug(f'[DotOutManager.__init__] processStage: {processStage}')
        log.debug(f'[DotOutManager.__init__] mongoDBupload: {mongoDBupload}')
        log.debug(f'[DotOutManager.__init__] MMSupload: {MMSupload}')
        log.debug(f'[DotOutManager.__init__] Out2EDCpath: {Out2EDCpath}')

        self.connection = connection
        self.blueprint = blueprint
        self.folderPath = folderPath
        self.mongoDBupload = mongoDBupload
        self.processStage = processStage
        self.MMSupload = MMSupload
        self.Out2EDCpath = Out2EDCpath

        if self.MMSupload is True and self.Out2EDCpath is None:

            benchConfig = getBenchConfig(hostname())
            pathString = benchConfig.get('localOut2EDCpath')
            if pathString is None:
                raise DotOutManagementException('MMSupload is True; Out2EDCpath not passed and not found in bench configuration.')
            self.Out2EDCpath = Path(pathString)
            log.info(f'Out2EDCpath not passed. Using default for bench: "{self.Out2EDCpath}"')
        
        if self.MMSupload is True:
            _checkExePath(self.Out2EDCpath)

        if self.folderPath is None:
            benchConfig = getBenchConfig(hostname())
            self.folderPath = Path(benchConfig['folderPath'])
            log.info(f'folderPath not passed. Using default for bench: "{self.folderPath}"')
            
    def _generateDotOutData(self, component) -> None:
        """Generates the dot-out data for the component (the datasheet), which
        is saved on the component itself. The data is not uploaded to the
        database (component is not mongoReplaced)."""
        generateComponentDotOutData(component, self.connection, self.blueprint, self.processStage)

    def _generateAndSaveDotOutData(self, component) -> None:
        """Generates the dot-out data for the component (the datasheet), which
        is saved on the component itself. The data is also uploaded to the
        database (component is mongoReplaced)."""
        generateAndSaveComponentDotOutData(component, self.connection, self.blueprint, self.processStage)

    @abstractmethod
    def _dotOutDF(self, component):
        """This method has to be implemented in the subclass. It must return
        the dot-out dataframe for the given component, and no other arguments
        can be used (pass them to the constructor instead).
        
        The method is called by the saveDotOutLine method, which is the main
        method of this class. It is used to generate the dot-out dataframe for
        the component, and then to save it to the .out file.
        
        The method has to return a DataFrame or None. If None is returned, the
        saveDotOutLine method will not save any data to the .out file for the
        component."""
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
            _logError(e)
            return None

    @abstractmethod
    def _dotOutFilePath(self, component) -> Path:
        """This method has to be implemented in the subclass. It must return
        the file path where the dot-out data for the component should be saved.
        No other arguments can be used (pass them to the constructor instead).
        
        The method is called by the saveDotOutLine method, which is the main
        method of this class. It is used to determine the file path where the
        dot-out data for the component should be saved.
        
        The method has to return a pathlib.Path object or None. If None is
        returned, the saveDotOutLine method will not save any data to the .out
        file for the component."""

        pass

    def __dotOutFilePath(self, component) -> Path:

        try:
            filePath = self._dotOutFilePath(component)

            if not isinstance(filePath, Path):
                raise DotOutManagementException(f'The _dotOutFilePath method of class {self.__class__.__name__} has not returned a pathlib.Path object (it returned {type(filePath)}).')

            return filePath
        
        except Exception as e:
            log.error(f'[DotOutManager] Something went wrong when determing the .out file path for component "{component.name}".')
            _logError(e)
            return None

    def _hasRelevantData(self, component) -> bool:
        """Returns True if the component has entries in its test history with
        data to be saved to the .out file, False otherwise.

        If the component has no testHistory, the method returns False.

        Then, if the manager processStage field is None, the method returns True.

        When processStage is different from None, this method searches the
        component history for entries that have that processStage. If any is
        found, the method returns True, otherwise it returns False.
        
        Returns True or False."""

        testHistory = component.getField('testHistory', verbose = False,
                                         valueIfNotFound = None,
                                         notFoundValues = [[], None])
        if testHistory is None:
            return False
        
        if hasattr(self, 'processStage'):
            stage = self.processStage
            if stage is None:
                return True
            else:
                stages = [stage]
        
        if hasattr(self, 'processStages'):
            stages = self.processStages
            if stages is None:
                return True
        
        for entry in testHistory:
            stage = entry.get('processStage')

            if stage in stages:
                return True

        return False

    def saveDotOutLine(self, component) -> Path:
        """This method is the main method of the class. It is used to generate
        the dot-out dataframe for the component, and then to save it to the .out
        file.

        This methods first generates the dot-out data for the component using
        the datasheet mechanism of mongomanager components.

        Then, the method generates the dot-out dataframe for the component using
        the _dotOutDF method, and then determines the file path where the data
        should be saved using the _dotOutFilePath method.

        Finally, the method appends the data to the .out file using the
        appendData method of the DotOutFileManager class.

        Returns the file path where the data has been saved, or None if the
        process has failed at any point.
        """

        log.important(f'Started saving dot out line for component "{component.name}".')

        if not self._hasRelevantData(component):
            log.warning(f'The component "{component.name}" has no relevant data to append on the dot out file.')
            return None

        try:
            if self.mongoDBupload is True:
                log.important(f'mongoDBupload = True: Generating datasheet for component and uploading to MongoDB.')
                self._generateAndSaveDotOutData(component)
            else:
                log.important(f'mongoDBupload = False: Generating datasheet for component (without uploading to MongoDB).')
                self._generateDotOutData(component)

        except FailedDatasheetGeneration as e:
            log.warning(f'The generation of the datasheet of component "{component.name}" has failed. Dot out line is not generated.')
            return None

        except Exception as e:
            log.error('[DotOutManager.saveDotOutLine] Something went wrong when generating and uploading datasheet data to MongoDB.')
            _logError(e)
            return None
        
        log.important(f'Generating dot out dataframe.')

        DF = self.__dotOutDF(component)
        if DF is None: return None

        filePath = self.__dotOutFilePath(component)
        if filePath is None: return None

        DotOutFileManager.appendData(filePath, DF)

        if self.MMSupload is True:
            log.important(f'Uploading to MMS.')
            runOut2EDC(self.Out2EDCpath, filePath)

        return filePath   

class DotOutManager_Modules(DotOutManager):
    """This class is used to manage the generation of dot-out tables for
    modules and for appending the corresponding data to the .out files.
    
    This class implements the _dotOutDF method from the parent abstract class
    DotOutManager by calling the _singleModuleDotOutDataFrame function, which
    generates the dot-out dataframe for a single module.
    """

    def __init__(self, connection, folderPath:Path = None,
                 blueprint:mom.blueprint = None,
                 processStage:str = None,
                 mongoDBupload:bool = False,
                 MMSupload:bool = True,
                 Out2EDCpath:Path = None,
                 allResultDigits:bool = False,
                 scientificNotationThreshold:float = 10**9,
                 ):
        """
        Constructor method (__init__) of DotOutManager_Modules.

        Args:
            connection (mom.connection): The connection to the MongoDB database.
            folderPath(pathlib.Path, optional): The path to the folder where the
                .out files are saved. If not passed, the default path for the
                bench is retrieved from MMSconnector_benchConfig.
            blueprint (mom.blueprint, optional): If passed, the datasheet
                definition is retrieved from the blueprint. Meant for testing
                purposes. Defaults to None.
            processStage (str, optional): The process stage for which the data
                is generated. If None, the data is generated for all process
                stages. Defaults to None.
            mongoDBupload (bool, optional): If True, the generated data (the
                component datasheet) is uploaded to the database. Defaults to
                False.
            MMSupload (bool, optional): If True, the line appended to the .out
                file is also uploaded to the MMS database using the Out2EDC
                executable. Defaults to True.
            Out2EDCpath (pathlib.Path, optional): The path to the local 
                Out2EDC.exe executable. If not passed, the default path for the
                bench is retrieved from MMSconnector_benchConfig. Defaults to
                None.
            allResultDigits (bool, optional): If False, float number decimal
                parts are truncated. If True, the whole number is reported.
            scientificNotationThreshold (float, optional): The threshold for the
                absolute value of numbers over which they are reported in scientific
                notation. Defaults to 10**9.
        """
        
        super().__init__(connection, folderPath, blueprint, processStage, mongoDBupload,
                         MMSupload, Out2EDCpath)
        self.allResultDigits = allResultDigits
        self.scientificNotationThreshold = scientificNotationThreshold

    @staticmethod
    def _isModuleLegacy(module) -> bool:

        if not "MMSrecord" in module.getField('supportDictionary', valueIfNotFound = {}, verbose = False):
            return False

        return 'SN' not in module.name

    @staticmethod
    def _processStageTag(processStage) -> str:
        """Returns the acronym for the process stage, as used in the MMS system."""

        return processStage.replace(' ', '-')

    def _dotOutFilePath(self, module) -> Path:
        
        if self._isModuleLegacy(module):
            batch = module.getField('batch', verbose = False)
            
            fileStem = batch.replace('/', '-') if batch is not None else 'NoBatch'
            
            if self.processStage is not None:
                fileStem += f'_{self._processStageTag(self.processStage)}_legacy'

        else:
            
            PN = module.getField(['supportDictionary', 'MMSrecord', 'MMSinfo', 'P\\N REV'])
            
            if PN is None: PN = 'NoPN'
            
            fileStem = PN
            
            if self.processStage is not None:
                fileStem += f'_{self._processStageTag(self.processStage)}'

        fileName = fileStem + '.out'
        filePath = self.folderPath / fileName
        return filePath


    def _dotOutDF(self, module) -> DataFrame:

        if not self._isModuleLegacy(module):
            DF = _singleModuleDotOutDataFrame_CDM128(self.connection, module,
                blueprint = self.blueprint,
                allResultDigits = self.allResultDigits,
                scientificNotationThreshold = self.scientificNotationThreshold)
        else:
            DF = _singleModuleDotOutDataFrame_legacy(self.connection, module,
                blueprint = self.blueprint,
                allResultDigits = self.allResultDigits,
                scientificNotationThreshold = self.scientificNotationThreshold,
                batchCode = module.getField('batch', verbose = False))
        
        if DF is None: return None

        # Changing column names to append the stage acronym
        self._renameColumnsForStage(DF) # Works even if processStage is None

        return DF
    
    def _renameStageMap(self, DF:DataFrame) -> dict:
        """This method returns a dictionary that can be used to rename the
        columns of the dot-out dataframe to append the process stage acronym to
        them."""

        if self.processStage is None:
            raise DotOutManagementException('No process stage has been defined, thus the stage map cannot be generated.')
        
        stageTag = self._processStageTag(self.processStage)
        
        oldColumns = list(DF.columns)

        notToRenameColumns = ['LOTID', 'S\\N', 'P\\N REV', 'CHIP ID',
                              'SUBSTRATE', 'PACKAGE ID', 'PROTOTYPE NOTE',
                              'PRODUCTION ORDER', 'Module_Name', 'MongoDB_ID']

        newColumns = []
        for col in oldColumns:
            if col in notToRenameColumns:
                newColumns.append(col)
            else:
                newColumns.append(f'{col}_{stageTag}')

        return dict(zip(oldColumns, newColumns))
    
    def _renameColumnsForStage(self, DF:DataFrame) -> None:
        """This method renames the columns of the dot-out dataframe to append
        the process stage acronym to them."""

        if self.processStage is None:
            return
        
        else:
            _renameColumns(DF, self._renameStageMap(DF))
    

def modulePackageID(module) -> str:

    moduleName = module.name
    if moduleName is None: return ""

    match = re.search(r'K\d{5}', moduleName)
    return match.group(0) if match else ""
    


class DotOutManager_Chips(DotOutManager):
    """This class is used to manage the generation of dot-out tables for
    chips and for appending the corresponding data to the .out files.
    
    This class implements the _dotOutDF method from the parent abstract class
    DotOutManager by calling the _singleChipDotOutDataFrame function, which
    generates the dot-out dataframe for a single chip.

    This class also renames some of the columns of the dot-out dataframe as
    generated by the _singleChipDotOutDataFrame function, to make the
    terminology consistent with what Production is using for the MMS database.

    The acronym columns are renamed to append the process stage acronym to them,
    as well as the ChipID, type, LOT_ID, and DUT_ID columns.
    """

    def __init__(self, connection, folderPath:Path = None,
                 blueprint:mom.blueprint = None,
                 processStage:str = None,
                 mongoDBupload:bool = False,
                 MMSupload:bool = True,
                 Out2EDCpath:Path = None,
                 allResultDigits:bool = False,
                 scientificNotationThreshold:float = 10**9,
                 ):
        """
        Constructor method (__init__) of DotOutManager_Chips.

        N.B. This class is meant to be used for optical chips only, and only
        when a single process stage string is of interest.
        
        For Cordoba chips only, multiple process stages are supported by using
        the dotOutManager_MixedStagesCordobaChips class.

        Args:
            connection (mom.connection): The connection to the MongoDB database.
            folderPath(pathlib.Path, optional): The path to the folder where the
                .out files are saved. If not passed, the default path for the
                bench is retrieved from MMSconnector_benchConfig.
            blueprint (mom.blueprint, optional): If passed, the datasheet
                definition is retrieved from the blueprint. Meant for testing
                purposes. Defaults to None.
            processStage (str, optional): The process stage for which the data
                is generated. If None, the data is generated for all process
                stages. Defaults to None.
            mongoDBupload (bool, optional): If True, the generated data (the
                component datasheet) is uploaded to the database. Defaults to
                False.
            MMSupload (bool, optional): If True, the line appended to the .out
                file is also uploaded to the MMS database using the Out2EDC
                executable. Defaults to True.
            Out2EDCpath (pathlib.Path, optional): The path to the local 
                Out2EDC.exe executable. If not passed, the default path for the
                bench is retrieved from MMSconnector_benchConfig. Defaults to
                None.
            allResultDigits (bool, optional): If False, float number decimal
                parts are truncated. If True, the whole number is reported.
            scientificNotationThreshold (float, optional): The threshold for the
                absolute value of numbers over which they are reported in scientific
                notation. Defaults to 10**9.
        """
        
        super().__init__(connection, folderPath, blueprint, processStage,
                         mongoDBupload, MMSupload, Out2EDCpath)
        
        self.allResultDigits = allResultDigits
        self.scientificNotationThreshold = scientificNotationThreshold

        log.debug('[DotOutManager_Chips.__init__] DotOutManager_Chips initialized.')
        log.debug(f'[DotOutManager_Chips.__init__] allResultDigits: {allResultDigits}')
        log.debug(f'[DotOutManager_Chips.__init__] scientificNotationThreshold: {scientificNotationThreshold}')

    @staticmethod
    def _processStageTag(processStage) -> str:
        """Returns the acronym for the process stage, as used in the MMS system."""

        if processStage in STAGES_MMS:
            return processStage

        if processStage in STAGES_MONGODB_TO_MMS:
            return STAGES_MONGODB_TO_MMS[processStage]

        log.warning(f'Process stage "{processStage}" not recognized. Using as is (recognized stages: {STAGES_MMS + list(STAGES_MONGODB_TO_MMS.keys())}).')
        return processStage.replace(' ', '-')

    def _dotOutFilePath(self, chip) -> Path:

        waferName, _ = chip.name.split('_', maxsplit = 1)
        waferType = waferName[1:3]

        if waferType in PART_NUMBERS:
            fileStem = PART_NUMBERS[waferType]

        else:
            # File name generated from wafer name
            fileStem = waferName

        if self.processStage is not None:
            fileStem += f'_{self._processStageTag(self.processStage)}'

        fileName = fileStem + '.out'
        filePath = self.folderPath / fileName
        return filePath

    def _dotOutDF(self, chip) -> DataFrame:

        # with mom.recallDocuments():
        wafer = chip.ParentComponent.retrieveElement(self.connection)
        if wafer is None:
            log.warning(f'Could not retrieve wafer for chip "{chip.name}".')
            waferName = '<No wafer name>'
        else:
            waferName = wafer.name

        chipName = chip.name

        DF = _singleChipDotOutDataFrame(self.connection, chip,
                blueprint = self.blueprint,
                allResultDigits = self.allResultDigits,
                scientificNotationThreshold = self.scientificNotationThreshold,
                waferName = waferName)
        if DF is None: return None

        # Parsing general information for MMS

        _prependConstantColumn(DF, 'ChipID', chipID(chipName))
        _prependConstantColumn(DF, 'type', chipType(chipName))
        _prependConstantColumn(DF, 'LOT_ID', LOT_ID(chipName))
        _prependConstantColumn(DF, 'DUT_ID', DUT_ID(chipName))
        _deleteColumns(DF, ['waferName', 'component', 'componentID'])

        # Changing column names to append the stage acronym
        self._renameColumnsForStage(DF) # Works even if processStage is None

        return DF
    
    def _renameStageMap(self, DF:DataFrame) -> dict:
        """This method returns a dictionary that can be used to rename the
        columns of the dot-out dataframe to append the process stage acronym to
        them."""

        if self.processStage is None:
            raise DotOutManagementException('No process stage has been defined, thus the stage map cannot be generated.')
        
        stageTag = self._processStageTag(self.processStage)
        
        oldColumns = list(DF.columns)

        newColumns = []
        for col in oldColumns:
            if col in ['DUT_ID', 'LOT_ID', 'type', 'ChipID']:
                newColumns.append(col)
            else:
                newColumns.append(f'{col}_{stageTag}')

        return dict(zip(oldColumns, newColumns))
    
    def _renameColumnsForStage(self, DF:DataFrame) -> None:
        """This method renames the columns of the dot-out dataframe to append
        the process stage acronym to them."""

        if self.processStage is None:
            return
        
        else:
            _renameColumns(DF, self._renameStageMap(DF))


class dotOutManager_MixedStagesCordobaChips(DotOutManager_Chips):

    def __init__(self, connection, folderPath:Path = None,
                 blueprint:mom.blueprint = None,
                 processStage_orStages:str|list[str] = None,
                 mongoDBupload:bool = False,
                 MMSupload:bool = True,
                 Out2EDCpath:Path = None,
                 allResultDigits:bool = False,
                 scientificNotationThreshold:float = 10**9):
        """
        Constructor method (__init__) of dotOutManager_MixedStagesCordobaChips.

        N.B. This class should be used only with Cordoba chips.
        Note that mixed stages are related to the STAGES_MONGODB_TO_MMS
        dictionary defined at the beginning of this module.

        Args:
            connection (mom.connection): The connection to the MongoDB database.
            folderPath(pathlib.Path, optional): The path to the folder where the
                .out files are saved. If not passed, the default path for the
                bench is retrieved from MMSconnector_benchConfig.
            blueprint (mom.blueprint, optional): If passed, the datasheet
                definition is retrieved from the blueprint. Meant for testing
                purposes. Defaults to None.
            processStage_orStages (str|list[str], optional): The process stage 
                or stages for which the data is generated. If None, the data is
                generated for all process stages. If multiple stages are passed,
                they should be different strings that represent the same stage.
                Defaults to None.
            mongoDBupload (bool, optional): If True, the generated data (the
                component datasheet) is uploaded to the database. Defaults to
                False.
            MMSupload (bool, optional): If True, the line appended to the .out
                file is also uploaded to the MMS database using the Out2EDC
                executable. Defaults to True.
            Out2EDCpath (pathlib.Path, optional): The path to the local 
                Out2EDC.exe executable. If not passed, the default path for the
                bench is retrieved from MMSconnector_benchConfig. Defaults to
                None.
            allResultDigits (bool, optional): If False, float number decimal
                parts are truncated. If True, the whole number is reported.
            scientificNotationThreshold (float, optional): The threshold for the
                absolute value of numbers over which they are reported in scientific
                notation. Defaults to 10**9.
        """

        super().__init__(connection, folderPath,
                         blueprint, processStage_orStages, mongoDBupload,
                         MMSupload, Out2EDCpath,
                         allResultDigits, scientificNotationThreshold)

        if self.processStage is None:
            self.processStages = None
        if isinstance(self.processStage, list):
            self.processStages = self.processStage
        else:
            self.processStages = [self.processStage]

        log.debug(f'[dotOutManager_MixedStagesCordobaChips.__init__] dotOutManager_MixedStagesCordobaChips initialized.')
        log.debug(f'[dotOutManager_MixedStagesCordobaChips.__init__] processStages: {self.processStages}')


    # @classmethod
    def _processStageTag(cls, processStage) -> str:
        
        superTag = super()._processStageTag

        if isinstance(processStage, list):
            
            for stage in processStage:
                if stage in STAGES_MMS:
                    return stage
                if stage in STAGES_MONGODB_TO_MMS:
                    return STAGES_MONGODB_TO_MMS[stage]
                
            return '_'.join([superTag(stage) for stage in processStage])
        else:
            return superTag(processStage)


    def _generateDotOutData(self, component):
        # Passing self.processStages instead of self.processStage
        generateComponentDotOutData(component, self.connection, self.blueprint, self.processStages)

    def _generateAndSaveDotOutData(self, component):
        # Passing self.processStages instead of self.processStage
        generateAndSaveComponentDotOutData(component, self.connection, self.blueprint, self.processStages)

# ==============================================================================
# Running Out2EDC
        
def _checkDotOutPath(dotOutPath:Path) -> None:
    
    if not isinstance(dotOutPath, Path):
        raise TypeError("dotOutPath must be a pathlib.Path object.")
    if not dotOutPath.suffix == ".out":
        raise ValueError("dotOutPath must point to an .out file.")

def _checkExePath(exePath:Path) -> None:
    
    if not isinstance(exePath, Path):
        raise TypeError("exePath must be a pathlib.Path object.")
    if not exePath.suffix == ".exe":
        raise ValueError("exePath must point to an .exe file.")
    if not exePath.stem == 'Out2EDC':
        raise ValueError('exePath must point to "Out2EDC.exe".')
    if not exePath.exists():
        raise FileNotFoundError(f'File "{exePath}" does not exist.')
    
def _determineDotOutLogPath(dotOutLogFolder:Path) -> Path:
    """The log file is determined based on the hostname of the machine where the
    script is running and from the month and year of the current date."""

    if not isinstance(dotOutLogFolder, Path):
        raise TypeError(f"dotOutLogFolder must be a pathlib.Path object (it is {type(dotOutLogFolder)}).")

    fileStem = f'DotOutGenerationLog_{gethostname().upper()}_' + mom.awareNow().strftime('%Y-%m') + '.log'

    fileStem = '_'.join([
        'DotOutGenerationLog',
        gethostname().upper(),
        mom.awareNow().strftime('%Y-%m'),
    ]) + '.log'

    return dotOutLogFolder / fileStem

def _logExecutionOfOut2EDC(successfully:bool, dotOutLogPath:Path = None) -> None:

    if dotOutLogPath is not None:
        if not isinstance(dotOutLogPath, Path):
            raise TypeError(f"dotOutLogPath must be a pathlib.Path object or None (it is {type(dotOutLogPath)}).")
    if not isinstance(successfully, bool):
        raise TypeError(f"succesfully must be a boolean (it is {type(successfully)}).")

    if dotOutLogPath is None:
        dotOutLogPath = _determineDotOutLogPath(DOT_OUT_LOG_FOLDER)

    if successfully is True:
        message = f'{mom.awareNow()} - {hostname()} - OUT2EDC successfully executed.'
    else:
        message = f'{mom.awareNow()} - {hostname()} - OUT2EDC execution failed.'

    try:
        with open(dotOutLogPath, 'a') as fileOut:
            fileOut.write(message + '\n')
    except Exception as e:
        log.error(f'An error occurred when logging the execution of OUT2EDC to file "{dotOutLogPath}": {e}.')

            
def runOut2EDC(exePath:Path, dotOutPath:Path,
               *, raiseExceptions:bool = True) -> None:

    _checkExePath(exePath)
    _checkDotOutPath(dotOutPath)
    
    # command = f'"{exePath}" "{dotOutPath}"'
    command = [f'{exePath}', f'{dotOutPath}']

    try:
        run(command, shell = True, capture_output=True, check = True, timeout=10)

    except CalledProcessError as e:
        log.error('An error occurred with running OUT2EDC.exe')
        log.error(f'Execution exit code: {e.returncode}')
        log.error('STDOUT: ' + e.stdout.decode('utf-8'))
        log.error('STDERR: ' + e.stderr.decode('utf-8'))
        log.error(f"Python exception: {e}")
        _logExecutionOfOut2EDC(False)
        if raiseExceptions: raise e

    except TimeoutExpired as e:
        log.error('Timeout (5 sec) reached when running Out2EDC.exe')
        log.error('STDOUT: ' + e.stdout.decode('utf-8'))
        log.error('STDERR: ' + e.stderr.decode('utf-8'))
        log.error(f"Python exception: {e}")
        _logExecutionOfOut2EDC(False)
        if raiseExceptions: raise e

    except Exception as e:
        log.error(f"An error occurred with running OUT2EDC.exe: {e}")
        _logExecutionOfOut2EDC(False)
        if raiseExceptions: raise e

    else:
        log.info(f"OUT2EDC successfully executed ({command}).")
        _logExecutionOfOut2EDC(True)