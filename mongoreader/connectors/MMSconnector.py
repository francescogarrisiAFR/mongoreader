from mongoutils import queryUtils as qu
import mongoreader.wafers as morw
import mongoreader.modules as morm
from datautils import dataClass
import mongomanager as mom
from mongomanager.errors import FieldNotFound
from mongomanager import log
from pandas import DataFrame, concat

from pathlib import Path
from abc import ABC, abstractmethod


def acronymsFromDSdefinition(DSdefintion:list, locGroupDict:dict):
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
    
    return acronymsFromDSdefinition(DSDefinition, groupsDict)


def spawnEmptyDataFrame(columnNames:list) -> DataFrame:
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


def spawnEmptyDotOutDataframe(acronyms:list) -> DataFrame:
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

    return spawnEmptyDataFrame(
        ['component', 'componentID', 'status', 'processStage',
        'earliestTestDate', 'latestTestDate'] + acronyms)


def generateComponentDotOutData(component, connection) -> None:

    component.Datasheet.createAndStoreDatasheet(connection,
                                requiredProcessStages = None,
                                requiredStati = None)


def generateAndSaveComponentDotOutData(component, connection) -> None:

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

    The data for populating the DataFrame may be passed separetly from the
    component.

    Args:
        emptyDotOutDataFrame (DataFrame): The empty DataFrame to be populated
            with data from component. See spawnEmptyDotOutDataframe().
        component (mom.component): The component from which are retrieved the
            data that go to populate the DataFrame.
        componentDotOutData (DataFrame, optional): If passed, the DataFrame data
            are not retrieved from the component, but they are taken from this
            DataFrame. Defaults to None.
    
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
        raise TypeError(f'"component" must be a mongomanager.component object.')
    
    if not isinstance(emptyDotOutDataFrame, DataFrame):
        raise TypeError(f'"emptyDotOutDataFrame" must be a DataFrame.')
    
    if not isinstance(componentDotOutData, DataFrame):
        raise TypeError(f'"componentDotOutData" must be a DataFrame or None.')

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
        
    print(f'DEBUG: rowDict generated ({rowDict})')
    
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


def groupedDotOutDataFrame(components:list, blueprints:list = None,
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
        emptyDotOutDataFrame = spawnEmptyDotOutDataframe(acronyms),
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


def moduleBatchDotOutDataFrame(connection, batch:str, * ,
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

    dataFrame = groupedDotOutDataFrame(mods, bps,
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


def waferCollationDotOutDataFrame(connection, waferName:str, *,
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
    

    dataFrame = groupedDotOutDataFrame(wc.chips, wc.chipBlueprints,
                                       allResultDigits = allResultDigits,
                                       scientificNotationThreshold = scientificNotationThreshold)

    # Adding information relative to the batch

    # Prepending the batch column
    dataFrame.insert(0, "waferID", len(dataFrame)*[wc.wafer.ID])
    dataFrame.insert(0, "wafer", len(dataFrame)*[waferName])

    dataFrame.rename(columns={"component": "chip",
                              "componentID": "chipID"}, inplace=True)

    return dataFrame


def prependConstantColumn(DF:DataFrame, columnName:str, columnValue) -> None:
    DF.insert(0, "waferID", len(DF)*[columnValue])

def appendConstantColumn(DF:DataFrame, columnName:str, columnValue) -> None:
    DF.insert(len(DF.columns), "waferID", len(DF)*[columnValue])

def renameColumns(DF:DataFrame, renamingMap:str) -> None:
    DF.rename(renamingMap)


def singleComponentDotOutDataFrame(connection, component, *,
                                allResultDigits:bool = False,
                                scientificNotationThreshold:float = 10**9,
                            ):
    
    bp = component.retrieveBlueprint(connection)
    if bp is None:
        raise Exception(f'Could not generate dot out dataframe because component "{component.name}" has no associated blueprint.')
    
    acronyms = acronymsFromBlueprint(bp)
    emptyDF = spawnEmptyDotOutDataframe(acronyms)

    dotOutData = retrieveComponentDotOutData(component)

    DF = dotOutDataFrame(emptyDF, component, dotOutData,
                         allResultDigits=allResultDigits,
                         scientificNotationThreshold=scientificNotationThreshold)
    
    return DF


def singleChipDotOutDataFrame(connection, component, *,
                            allResultDigits:bool = False,
                            scientificNotationThreshold:float = 10**9,
                            waferName:str):

    DF = singleComponentDotOutDataFrame(connection, component,
        allResultDigits=allResultDigits,
        scientificNotationThreshold=scientificNotationThreshold)

    prependConstantColumn(DF, 'waferName', waferName)

    return DF


def singleModuleDotOutDataFrame(connection, component, *,
                            allResultDigits:bool = False,
                            scientificNotationThreshold:float = 10**9,
                            batchCode:str):
    
    DF = singleComponentDotOutDataFrame(connection, component,
        allResultDigits=allResultDigits,
        scientificNotationThreshold=scientificNotationThreshold)

    prependConstantColumn(DF, 'batch', batchCode)

    return DF


# ==============================================================================
# Dot Out file management

class DotOutManagementException(Exception):
    pass

class DotOutFileManager:
    
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
        
        header, data = cls._extractHeaderData(singleComponentDotOutDF)

        print(f'Header: {header}')
        print(f'Data  : {data}')

        if not filePath.exists():
            cls._createDotOutFile(filePath, header)

        cls._writeLine(filePath, data)
        print('(Written data)')
        
        print(f'{filePath}')
    


# =============================================================================
# Global manager

class DotOutManager(ABC):

    def __init__(self, connection, folderPath:Path):
        pass

        self.connection = connection
        self.folderPath = folderPath

    def generateDotOutData(self, component):
        generateComponentDotOutData(component, self.connection)

    def generateAndSaveDotOutData(self, component):
        generateAndSaveComponentDotOutData(component, self.connection)

    @abstractmethod
    def dotOutDF(self, component):
        pass

    @abstractmethod
    def dotOutFilePath(self) -> Path:
        pass

    def saveDotOutLine(self, component, mongoDBupload:bool = False):
        
        if mongoDBupload:
            self.generateAndSaveDotOutData(component)
        
        else:
            self.generateDotOutData(component)
        
        DF = self.dotOutDF(component)
        filePath = self.dotOutFilePath()

        DotOutFileManager.appendData(filePath, DF)


class DotOutManager_Modules(DotOutManager):
    pass

    # TODO: Rename columns for modules

class DotOutManager_CDM128Modules(DotOutManager_Modules):
    pass

    # TODO: Rename columns for CDM128 modules


class DotOutManager_Chips(DotOutManager):
    pass

    # TODO: Rename columns for chips

    # “wafer” (o “waferName”): 3CA0039  “LotID”: 3CA0039
    # “chip” (o “chipName”): COR-V2-02  “ChipID”: 32 (codifica necessaria solo ora per Cordoba per via delle revisioni, in seguito il ChipID sarà univoco per la fetta come lo è già ad esempio per Cambridge) 
    # DUT_ID  LotID+ChipID = 3CA003932 (prima colonna da riportare nel file .out attualmente generato)


class DotOutManager_CordobaChips(DotOutManager_Chips):
    pass

    # TODO: Rename columns for cordoba chips