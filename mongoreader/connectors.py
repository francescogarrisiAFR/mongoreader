from mongoutils import queryUtils as qu
import mongoreader.wafers as morw
import mongoreader.modules as morm
from datautils import dataClass
import mongomanager as mom
from mongomanager.errors import FieldNotFound
from mongomanager import log
from pandas import DataFrame, concat


SCIENTIFIC_NOTATION_THRESHOLD = 1e9

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


def addConstantColumnToDataFrame(DF:DataFrame, columnName:str, columnValue, index:int = 0) -> list:
    
    DF.insert(index, columnName, [columnValue]*len(DF))
    return DF

class testReportAnalyzer:

    def __init__(self, testReport):
        self.testReport = testReport

    def generalInfo(self) -> dict:

        # General information
        generalInfo = {
            'testReportName': self.testReport.getField('name', verbose = False),
            'testReportID': self.testReport.ID,
            'componentID': self.testReport.getField('componentID', verbose = False),
            'status': self.testReport.getField('componentStatus', verbose = False),
            'processStage': self.testReport.getField('componentProcessStage', verbose = False),
            'executionDate': self.testReport.getField('executionDate', verbose = False),
        }

        # Could add information regarding the test bench, the operator, etc.

        return generalInfo

    @staticmethod
    def _resultValueRepr(resultValue = None, resultError = None):

        if resultValue is not None:
            if abs(resultValue) > SCIENTIFIC_NOTATION_THRESHOLD:
                resultValue = str(resultValue)
                return resultValue

        resRepr = dataClass.valueErrorRepr(resultValue, resultError, valueDecimalsWithNoneError=2, printErrorPart=False)
        return resRepr

    @classmethod
    def _normalizeResult(cls, resultDict:dict) -> dict:

        resultDict['resultName'] = resultDict.get('resultName', None)

        resData = resultDict.get('resultData', None)
        resultDict['resultData'] = resData
        
        if resData is None:
            resultDict['resultValue'] = None
            resultDict['resultError'] = None
            resultDict['resultUnit'] = None
        else:
            value = resData.get('value', None)
            error = resData.get('error', None)
            unit = resData.get('unit', None)

            # Normalizing result
                
            if value is not None:
                if not isinstance(value, (int, float)):
                    log.error(f'Result value is not a number (int or float); resultValue = {value} (type: {type(value)}).')
                    value = None
            
            if error is not None:
                if not isinstance(error, (int, float)):
                    log.error(f'Result value is not a number (int or float); resultError = {error} (type: {type(error)}).')
                    error = None
            
            if unit is not None:
                if not isinstance(unit, str):
                    log.error(f'Result unit is not a string; resultUnit = {unit} (type: {type(unit)}).')
                    unit = None

            resultDict["resultData"] = {'value': value, 'error': error, 'unit': unit}
            resultDict['resultValue'] = value
            resultDict['resultError'] = error
            resultDict['resultUnit'] = unit
            
        
        resultDict['resultRepr'] = cls._resultValueRepr(value, error)
        resultDict['location'] = resultDict.get('location', None)
        resultDict['resultTags'] = resultDict.get('resultTags', None)
        resultDict['testParameters'] = resultDict.get('testParameters', None)
        resultDict['testConditions'] = resultDict.get('testConditions', None)

        return resultDict

    def resultsInfo(self) -> list:

        results = self.testReport.getField('results', verbose = False)
        if results is None:
            log.warning(f'No results found for test report "{self.testReport.name}" ({self.testReport.ID}).')
            return None

        results = [self._normalizeResult(result) for result in results]

        # Adding index information
        results = [dict(result, **{'resultIndex': index}) for index, result in enumerate(results)]

        return results
    
    def flattenedInfo(self) -> list:
        return [{**self.generalInfo(), **resInfo} for resInfo in self.resultsInfo()]



class componentDotOutGenerator:

    def __init__(self,
                 connection:mom.connection,
                 component:mom.component,
                 acronyms:list
                 ):

        self.connection = connection
        self.component = component
        self.acronyms = acronyms
        self.__sortingDictionary = self._defineSortingDictionary(acronyms)

    @staticmethod
    def _unpackAcronym(acronym:str):

        # Unpacking acronym
        acronym = acronym.split('_')
        resName = acronym[0]
        loc = acronym[1]
        reqTags = acronym[2:]

        return resName, loc, reqTags

    @classmethod
    def _defineSortingDictionary(cls, acronyms) -> dict:
        """Returns a map between result names, locations and required tags
        to the corresponding acronym.

        >>> {
        >>>     (resultName, location): {
        >>>         tuple[reqTags]: "resultName_location_reqTag1_reqTag2_...",
        >>>         tuple[reqTags]: "resultName_location_reqTag1_reqTag2_...",
        >>>         ...
        >>>         }
        >>>     ...     
        >>> }

        Returns:
            dict: Described above.
        """        

        sortingDict = {}

        for acr in acronyms:

            resName, loc, reqTags = cls._unpackAcronym(acr)
            majorKey = (resName, loc)
            minorKey = tuple(reqTags)

            if minorKey == (): # No required tags
                raise Exception(f'No required tags found for acronym "{acr}".')

            if majorKey not in sortingDict.keys():
                sortingDict[majorKey] = {}
            
            sortingDict[majorKey][minorKey] = acr
        
        return sortingDict

    @property
    def _sortingDictionary(self):
        return self.__sortingDictionary

    def _retrieveTestReports(self, connection) -> list:
        """Retrieves the test reports associated to the component."""

        testHistory = self.component.getField('testHistory', verbose = False)

        if testHistory is None:
            log.warning(f'No test history found for component "{self.component.name}".')
            return None

        # List of test report IDs
        trpIDs = [entry.get('testReportID') for entry in testHistory]
        trpIDs = list(set(trpIDs))
        while None in trpIDs:
            trpIDs.remove(None)

        # CHECK IF ORDER IS RESPECTED
        trps = mom.testReport.query(connection, {'_id': {'$in': trpIDs}})
        return trps
    

    def _generateDotOutHeader(self) -> DataFrame:
        return spawnEmptyDataFrame(['component', 'componentID', 'testReportName', 'testReportID',
                                    'status', 'processStage', 'executionDate',
                                    'resultIndex', 'testParameters', 'testConditions', ] \
                                    + self.acronyms)
    
    def _acronymFromResult(self, resultInfo:dict) -> str:

        resName = resultInfo['resultName']
        resLocation = resultInfo['location']
        resTags = resultInfo['resultTags']

        majorKey = (resName, resLocation)

        sortingDict = self._sortingDictionary

        if majorKey not in sortingDict.keys():
            return None
        
        possibleMinorKeys = sortingDict[majorKey].keys()
        
        for minorKey in possibleMinorKeys:
            reqTags = minorKey

            if resTags is None: resTags = []
            
            if all([tag in resTags for tag in reqTags]):
                # Here I identified a result

                acronym = sortingDict[majorKey][minorKey]
                return acronym
            
                # TODO Check if other acronyms are possible

        return None

    def _resultsDict(self, report:testReportAnalyzer) -> dict:

        resDict = {}

        for result in report.resultsInfo():
            acronym = self._acronymFromResult(result)
            if acronym is None:
                continue
            resDict[acronym] = result['resultRepr']

        return resDict
    

    def _dotOutRowDict(self, testReport) -> dict:

        tra = testReportAnalyzer(testReport)

        reportGeneralInfo = tra.generalInfo()
        resultsDict = self._resultsDict(tra)

        # TODO: Update!
        generalInfoDict = {
            **{'component': self.component.name},
            **reportGeneralInfo.copy() 
        }

        rowDict = {**generalInfoDict, **resultsDict}
        return rowDict

    def _populateDotOutTable(self, dotOutHeader:DataFrame, testReports:list) -> DataFrame:
        
        DFs = []

        for trp in testReports:
            DF = dotOutHeader.copy()
            rowDict = self._dotOutRowDict(trp)
            DF.iloc[-1] = rowDict
            DFs.append(DF)

        if DFs == []:
            return None

        return concat(DFs)
    
    def _renameDFcolumns(self, dotOutTable:DataFrame,
            renameMap:dict = None
        ) -> DataFrame:

        # No columns to rename
        if renameMap is None:
            return dotOutTable

        dotOutTable.rename(columns = renameMap, inplace = True)
        return dotOutTable

    def generateDotOut(self) -> DataFrame:

        dotOutHeader = self._generateDotOutHeader()
        
        testReports = self._retrieveTestReports(self.connection)
        if testReports is None:
            log.info(f'No test reports found for component "{self.component.name}".')
            return None

        dotOutTable = self._populateDotOutTable(dotOutHeader, testReports)
        if dotOutTable is None:
            log.error(f'No results found for component "{self.component.name}".')
            return None
        
        dotOutTable = self._renameDFcolumns(dotOutTable)

        return dotOutTable
    

class chipDotOutGenerator(componentDotOutGenerator):
    
    def _renameDFcolumns(self, dotOutTable: DataFrame, renameMap: dict = {'component': 'chip', 'componentID': 'chipID' }) -> DataFrame:
        return super()._renameDFcolumns(dotOutTable, renameMap)

class moduleDotOutGenerator(componentDotOutGenerator):
    
    def _renameDFcolumns(self, dotOutTable: DataFrame, renameMap: dict = { 'component': 'module', 'componentID': 'moduleID' }) -> DataFrame:
        return super()._renameDFcolumns(dotOutTable, renameMap)



class collectiveDotOutGenerator:

    def __init__(self, connection, components:list, DatasheetDefinition:list = None):

        self.connection = connection

        if DatasheetDefinition is not None:
            acronyms = acronymsFromDSdefinition(DatasheetDefinition)
        
        else:
            bps = self._retrieveComponentBlueprints(connection, components)

            if bps is None:
                raise Exception('No blueprints found for the components.')
            
            if len(bps) > 1:
                log.warning('Multiple blueprints found for the components.')

            acronyms = self._generateAcronyms_fromBlueprints(bps)

        self.components = components
        self.acronyms = acronyms

    def _generateComponentDotOuts(self, generator:componentDotOutGenerator) -> list:
        
        DFs = []

        with mom.opened(self.connection):
            for component in self.components:
                dotOutGenerator = generator(self.connection, component, self.acronyms)
                DF = dotOutGenerator.generateDotOut()
                if DF is not None:
                    DFs.append(DF)
        
        if DFs == []:
            return None

        return DFs
    
    @staticmethod
    def _retrieveComponentBlueprints(connection, components) -> list:

        bpIDs = [cmp.getField('blueprintID', verbose = False) for cmp in components]
        bpIDs = list(set(bpIDs))
        while None in bpIDs:
            bpIDs.remove(None)
        
        bps = mom.blueprint.query(connection, {'_id': {'$in': bpIDs}})
        return bps

    @staticmethod
    def _generateAcronyms_fromBlueprints(blueprints):

        allAcronyms = []

        for bp in blueprints:
            acronyms = acronymsFromBlueprint(bp)
            
            if acronyms is None:
                continue
        
            newAcronyms = [acr for acr in acronyms if acr not in allAcronyms]
            allAcronyms += newAcronyms
        
        return allAcronyms
    
    @staticmethod
    def prependGeneralInfo(DF, generalInfoDict:dict) -> DataFrame:

        if generalInfoDict is None or generalInfoDict == {}:
            return DF

        keys = reversed(list(generalInfoDict.keys()))
        values = reversed(list(generalInfoDict.values()))

        for key, value in zip(keys, values):
            addConstantColumnToDataFrame(DF, key, value, index = 0)

        return DF

    def generateDotOut(self, generalInfoDict:dict = None) -> DataFrame:

        DFs = self._generateComponentDotOuts()
        if DFs is None:
            return None
        
        DF = concat(DFs)
        DF = self.prependGeneralInfo(DF, generalInfoDict)

        return DF



class waferCollationDotOutGenerator(collectiveDotOutGenerator):
    
    def __init__(self, connection, waferName:str):

        wc = morw.waferCollation(connection, waferName)
        self.waferName = wc.wafer.name
        self.waferID = wc.wafer.ID
        super().__init__(connection, wc.chips)

    def _generateComponentDotOuts(self) -> DataFrame:
        return super()._generateComponentDotOuts(generator = chipDotOutGenerator)
    
    def generateDotOut(self) -> DataFrame:
        return super().generateDotOut(generalInfoDict = {'wafer': self.waferName, 'waferID': self.waferID})

class moduleBatchDotOutGenerator(collectiveDotOutGenerator):

    def __init__(self, connection, batch:str):

        self.batch = batch
        mb = morm.moduleBatch(connection, batch)
        super().__init__(connection, mb.modules)
    
    def _generateComponentDotOuts(self) -> DataFrame:
        return super()._generateComponentDotOuts(generator = moduleDotOutGenerator)
    
    def generateDotOut(self) -> DataFrame:
        return super().generateDotOut(generalInfoDict = {'batch': self.batch})