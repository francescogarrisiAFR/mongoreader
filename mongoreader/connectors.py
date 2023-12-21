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

    return DataFrame(None, columns=columnNames, index = [0])


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

        return generalInfo

    @staticmethod
    def _resultValueRepr(resultData:dict):

        if resultData is None:
            return None
        
        resValue = resultData.get('value')
        resError = resultData.get('error')

        if abs(resValue) > SCIENTIFIC_NOTATION_THRESHOLD:
                resValue = str(resValue)

        resRepr = dataClass.valueErrorRepr(resValue, resError, valueDecimalsWithNoneError=2, printErrorPart=False)
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
            resultDict['resultValue'] = resData.get('value', None)
            resultDict['resultError'] = resData.get('error', None)
            resultDict['resultUnit'] = resData.get('unit', None)

        # Normalizing result
        
        resultDict['resultRepr'] = cls._resultValueRepr(resultDict.get('resultData', None))
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

        return results



class dotOutGenerator:

    def __init__(self,
                 connection:mom.connection,
                 component:mom.component,
                 acronyms:list
                 ):

        self.connection = connection
        self.component = component
        self.acronyms = acronyms


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
                                    'status', 'processStage', 'executionDate'] \
                                    + self.acronyms)
    

    @staticmethod
    def _unpackAcronym(acronym:str):

        # Unpacking acronym
        acronym = acronym.split('_')
        resName = acronym[0]
        loc = acronym[1]
        reqTags = acronym[2:]

        return resName, loc, reqTags


    def _sortingDictionary(self) -> dict:
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

        for acr in self.acronyms:

            resName, loc, reqTags = self._unpackAcronym(acr)
            majorKey = (resName, loc)
            minorKey = tuple(reqTags)

            if majorKey not in sortingDict.keys():
                sortingDict[majorKey] = {}
            
            sortingDict[majorKey][minorKey] = acr
        
        return sortingDict
    
    def _acronymFromResult(self, resultInfo:dict) -> str:

        resName = resultInfo['resultName']
        resLocation = resultInfo['location']
        resTags = resultInfo['resultTags']

        majorKey = (resName, resLocation)

        sortingDict = self._sortingDictionary()

        if majorKey not in sortingDict.keys():
            return None
        
        possibleMinorKeys = sortingDict[majorKey].keys()
        
        for minorKey in possibleMinorKeys:
            reqTags = minorKey
            
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

        return concat(DFs)
    
    def _renameDFcolumns(self, dotOutTable:DataFrame,
            renameMap:dict = {
                'componentID': 'chipID',
            }
        ) -> DataFrame:

        dotOutTable.rename(columns = renameMap, inplace = True)
        return dotOutTable

    def generateDotOut(self) -> DataFrame:

        dotOutHeader = self._generateDotOutHeader()
        testReports = self._retrieveTestReports(self.connection)
        dotOutTable = self._populateDotOutTable(dotOutHeader, testReports)
        dotOutTable = self._renameDFcolumns(dotOutTable)
        return dotOutTable