from mongoutils import queryUtils as qu
import mongoreader.wafers as morw
import mongoreader.modules as morm
from datautils import dataClass
import mongomanager as mom
from mongomanager.errors import FieldNotFound
from mongomanager import log
from pandas import DataFrame, concat


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
        raise TypeError(f'"blueprint" must be a mongomanager.blueprint object.')

    DSDefinition = blueprint.getDatasheetDefinition()
    GroupsDict = blueprint.Locations.retrieveGroupsDict()

    if DSDefinition is None:
        return None
    if GroupsDict is None:
        return None
    
    return acronymsFromDSdefinition(DSDefinition, GroupsDict)


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

    return DataFrame(columns=columnNames)


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


def retrieveComponentDotOutData(component:mom.component)-> DataFrame:
    """This function returns from the component the data suitable for populating
    the Dot-Out table.
    
    Currently it retrieves data from the last datasheetData defined for the
    compnent.

    Returns:
        DataFrame | None: The returned data, or None if not found.
    """
    return component.Datasheet.retrieveData(returnDataFrame = True, verbose = False)


def dotOutDataFrame(emptyDotOutDataFrame:DataFrame,
                    component:mom.component,
                    componentDotOutData:DataFrame = None,
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
            notation. Defaults to 10**9.s

    Returns:
        DataFrame: The dot-out DataFrame for the component.
    """

    # Type Checks

    if not isinstance(component, mom.component):
        raise TypeError(f'"component" must be a mongomanager.component object.')
    
    if not isinstance(emptyDotOutDataFrame, DataFrame):
        raise TypeError(f'"emptyDotOutDataFrame" must be a DataFrame.')
    
    if componentDotOutData is not None:
        if not isinstance(componentDotOutData, DataFrame):
            raise TypeError(f'"componentDotOutData" must be a DataFrame or None.')


    if componentDotOutData == None:
        # Retrieved from the chip itself
        componentDotOutData = retrieveComponentDotOutData(component)
    # If componentDotOutData is None I only retrieve global data

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

    dataFrame = DataFrame.from_dict({k: [v] for k, v in rowDict.items()})
    
    return concat([
            emptyDotOutDataFrame,
            dataFrame
        ],
        ignore_index=True)

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
                    returnType = 'Native', verbose = verbose)

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

    # I generate and concatenate the dot-out table for all the components
    groupedDataFrame = concat([dotOutDataFrame(
        emptyDotOutDataFrame = spawnEmptyDotOutDataframe(acronyms),
        component = cmp,
        componentDotOutData = None, # Generated from component
        allResultDigits = allResultDigits,
        scientificNotationThreshold = scientificNotationThreshold,
    ) for cmp in components])

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

