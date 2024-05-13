from .MMSconnectors_benchConfig import benchConfig
from datetime import datetime
from pandas import DataFrame, isnull
from pathlib import Path
import mongomanager as mom
from mongomanager import log
from mongomanager.goggleFunctions import componentGoggleFunctions as cmpGGF
import mongoreader.connectors.conversions as conv
from datautils import dataClass
from socket import gethostname

## TO DO ##

# 1. Logging and file logging
# 2. Add column for process stage

# --- Constants ---

SCIENTIFIC_NOTATION_THRESHOLD = 10**9

# --- Exceptions ---

class GoldenSampleReporterError(Exception):
    pass

class MissingInformation(GoldenSampleReporterError):
    pass

# --- Utility functions ---

def hostname() -> str:
    return gethostname().upper()

def getBenchConfig(hostname:str) -> dict:

    for config in benchConfig:
        if config['hostname'] == hostname:
            return config
    
    raise MissingInformation(f'No bench configuration found for hostname "{hostname}".')

def _isListOfDicts(arg) -> bool:
    if isinstance(arg, list):
        if all(isinstance(i, dict) for i in arg):
            return True
    return False

# --- Information retrieval functions ---

def _retrieveBlueprint(connection:mom.connection, component:mom.component) -> mom.blueprint:
    blueprint = component.retrieveBlueprint(connection)
    if blueprint is None:
        raise MissingInformation(f'Component "{component.name}" has no blueprint defined.')
    
    return blueprint

def _retrieveDatasheetDefinition(blueprint:mom.blueprint) -> list[dict]:
    DSD = blueprint.getDatasheetDefinition()
    if DSD is None:
        raise MissingInformation(f'Blueprint "{blueprint.name}" has no datasheet definition.')
    
    return DSD

def _retrieveLocationGroupsDict(blueprint:mom.blueprint) -> dict:
    
    groupsDict = blueprint.Locations.retrieveGroupsDict()
    if groupsDict is None:
        raise MissingInformation(f'Blueprint "{blueprint.name}" has no location groups defined.')
    
    return groupsDict

def _retrieveTestHistory(component:mom.component) -> list[dict]:
    testHist = component.getField('testHistory', verbose = False,
                                  notFoundValues = [[], None])
    if testHist is None:
        raise MissingInformation(f'Component "{component.name}" has no test history.')
    
    return testHist

def _datasheetElementsFromDefinition(DSDefinition:list[dict], locGroupDict:dict) -> list[dict]:

    if not _isListOfDicts(DSDefinition):
        raise TypeError(f'DSDefinition must be a list of dictionaries (it is {type(DSDefinition)}).')
    
    if not isinstance(locGroupDict, dict):
        raise TypeError(f'locGroupDict must be a dictionary (it is {type(locGroupDict)}).')
    
    allElements:list[dict] = []

    for entry in DSDefinition:

        resName = entry['resultName']
        locations = locGroupDict[entry['locationGroup']]

        reqTags = None
        if 'tagFilters' in entry:
            reqTags = entry['tagFilters'].get('required')
            if reqTags == []: reqTags = None

        if reqTags is None:
            log.warning(f'No required tags for result "{resName}" at location group "{entry["locationGroup"]}". Associated acronyms are generated without trailing tags.')

        exclTags = None
        if 'tagFilters' in entry:
            exclTags = entry['tagFilters'].get('toExclude')
            if exclTags == []: exclTags = None


        newElements = [{
                'resultName': resName,
                'location': loc,
                'requiredTags': reqTags,
                'tagsToExclude': exclTags,
            }
            for loc in locations]
        
        allElements.extend(newElements)
        
    return allElements


def _retrieveInformation(connection:mom.connection,
                        component:mom.component):
    """Retrieves all the relevant information from the component and blueprint
    that is necessary to generate the golden sample report."""
    
    if not isinstance(connection, mom.connection):
        raise TypeError(f'connection must be a mongomanager connection object (it is {type(connection)}).')
    
    if not isinstance(component, mom.component):
        raise TypeError(f'component must be a mongomanager component object (it is {type(component)}).')
    
    blueprint = _retrieveBlueprint(connection, component)
    DSDefinition = _retrieveDatasheetDefinition(blueprint)
    locGroupDict = _retrieveLocationGroupsDict(blueprint)
    DSElements = _datasheetElementsFromDefinition(DSDefinition, locGroupDict)
    testHistory = _retrieveTestHistory(component)
    
    return DSElements, testHistory

# --- Report raw filling functions ---

def _resultsFromTestHistoryEntry(entry:dict) -> list[dict]:
    """Returns a list of dictionaries containing the relevant information
    to be used to fill a raw of the golden sample report."""

    testResults = cmpGGF._scoopResultsFromTestEntry(entry)
    if testResults is None: testResults = []

    resultsDicts = []

    for res in testResults:
        dic = {
            'resultName': res.resultName,
            'location': res.location,
            'resultTags': res.resultTags,
            'resultValue': res.value,
            'resultError': res.error,
            # 'resultUnit': res.unit,
        }

        resultsDicts.append(dic)
    
    return resultsDicts

def _doResultAndElementMatch(resultDict:dict, DSelement:dict) -> bool:

    if resultDict['resultName'] != DSelement['resultName']:
        return False
    
    if resultDict['location'] != DSelement['location']:
        return False
    
    if DSelement['requiredTags'] is not None:
        if not all(tag in resultDict.get('resultTags', []) for tag in DSelement['requiredTags']):
            return False

    if DSelement['tagsToExclude'] is not None:
        if any(tag in resultDict.get('resultTags', []) for tag in DSelement['tagsToExclude']):
            return False

    return True

def _isNumberNone(number):
    """Assuming date is either None, a datetime object or a pandas.NaT object,
    this method returns True if date is None or pandas.NaT, False otherwise."""
    
    if number is None: return True
    if isnull(number): return True

    return False

def _normalizeValue(value, error) -> str:
    
    if _isNumberNone(value): value = None
    if _isNumberNone(error): error = None
    
    if value is None:
        return None
    
    if abs(value) > SCIENTIFIC_NOTATION_THRESHOLD:
        return str(value)

    value = dataClass.valueErrorRepr(value, error, valueDecimalsWithNoneError=2, printErrorPart=False)        
    return value

def _generateDataHeader(DSElements) -> list[str]:
    """Given elements in the form:
    
    >>> {
    >>>     'resultName': resName,
    >>>     'location': loc,
    >>>     'requiredTags': reqTags,
    >>> }

    this returns a list of strings that can be used as headers for the golden
    sample report.

    Each string is in the form:
    resultName_location_[requiredTags]
    where
    [requiredTags] is in turn a string containing the required tags separated
    by '_'.
    """

    def _stringFromElement(element:dict):

        string = '_'.join([
            element['resultName'],
            element['location']
        ])

        if element['requiredTags'] is not None:
            tagsString = '_'.join(element['requiredTags'])
            string += f'_{tagsString}'
        
        return string
    
    return [_stringFromElement(el) for el in DSElements]

def _generateDataRow(DSElements, testHistoryEntry:dict) -> list[dict]:
    
    rawValues = [None]*len(DSElements)

    for res in _resultsFromTestHistoryEntry(testHistoryEntry):
        for i, DSelement in enumerate(DSElements):
            if _doResultAndElementMatch(res, DSelement):
                
                rawValues[i] = _normalizeValue(res['resultValue'], res['resultError'])
                break
    
    return rawValues

def _generateMetadataHeader() -> list[str]:
    return ['DUT_ID', 'DATA_ORA', 'OP_NAME', 'BANCO', 'VER_SW', 'VER_PROF', 'PROCESS_STAGE']

def _generateChipID(componentName:str) -> str:
    return conv.Converter_dotOutDUTID.DUT_ID_fromMongoName(componentName)

def formatExecutionDate(date:datetime) -> str:
    return str(date)


def _generateMetadataRow(component:mom.component, testHistoryEntry:dict) -> list[str]:
    
    return [
        _generateChipID(component.name),
        formatExecutionDate(testHistoryEntry['executionDate']),
        None, # Operator name
        None, # Bench name
        None, # Software version
        None, # Profile version
        component.processStage
    ]


# --- Report generation functions ---

def _generateCompleteReport(connection:mom.connection,
                   component:mom.component) -> list[dict]:

    DSElements, testHistory = _retrieveInformation(connection, component)

    header = _generateMetadataHeader() + _generateDataHeader(DSElements)
    rows = [_generateMetadataRow(component, entry) + _generateDataRow(DSElements, entry)
            for entry in testHistory]

    records = [
        dict(zip(header, row))
        for row in rows
    ]

    return records

# --- Report saving functions ---

def _writeHeader(header:list[str], filePath:Path):

    if not isinstance(header, list):
        raise TypeError(f'header must be a list of strings (it is {type(header)}).')
    if not all(isinstance(s, str) for s in header):
        raise TypeError(f'Some elements of header are not strings.')

    if not isinstance(filePath, Path):
        raise TypeError(f'filePath must be a pathlib Path object (it is {type(filePath)}).')

    if filePath.exists():
        raise FileExistsError(f'File "{filePath}" already exists. Cannot overwrite.')
    
    with open(filePath, 'w', newline='') as outFile:
        outFile.write(','.join(header) + '\n\n')

def _appendReportLine(line:dict, filePath:Path):

    if not isinstance(line, dict):
        raise TypeError(f'line must be a dictionary (it is {type(line)}).')
    
    if not isinstance(filePath, Path):
        raise TypeError(f'filePath must be a pathlib Path object (it is {type(filePath)}).')

    lineDF = DataFrame([line])

    # Header
    header = list(line.keys())

    if not filePath.exists():
        _writeHeader(header, filePath)
        
    else:
        if not _checkReportHeader(header, filePath):
            raise GoldenSampleReporterError(f'Header in file "{filePath}" does not match expected header.')
        
    # Line
    with open(filePath, 'a', newline='') as outFile:
        lineDF.to_csv(outFile, index = False, header = False)

def _saveReportToCSV(report:list[dict], filePath:Path):
    
    header = list(report[0].keys())

    if not all(list(dic.keys()) == header for dic in report):
        raise GoldenSampleReporterError('All dictionaries in the report must have the same keys.')

    _writeHeader(header, filePath)

    for row in report:
        _appendReportLine(row, filePath)

# --- Report checking functions ---

def _readReportHeader(filename:Path) -> list[str]:
    with open(filename, 'r') as inFile:
        header = inFile.readline().strip().split(',')
    
    return header

def _checkReportHeader(expectedHeader:str, reportFilePath:Path) -> bool:
    return _readReportHeader(reportFilePath) == expectedHeader


# ------------------------------

# Manager class

class GoldenSampleReporter:

    def __init__(self,
                 connection:mom.connection,
                 component:mom.component,
                 folderPath:Path = None,
                 fileName:str = None):
        
        if not isinstance(connection, mom.connection):
            raise TypeError(f'connection must be a mongomanager connection object (it is {type(connection)}).')

        if not isinstance(component, mom.component):
            raise TypeError(f'component must be a mongomanager component object (it is {type(component)}).')
        
        if folderPath is not None:
            if not isinstance(folderPath, Path):
                raise TypeError(f'folderPath must be a pathlib Path object (it is {type(folderPath)}).')
        
        if fileName is not None:
            if not isinstance(fileName, str):
                raise TypeError(f'fileName must be a string (it is {type(fileName)}).')

        self._connection = connection
        self._component = component

        if folderPath is None: folderPath = self._autodetermineFolderPath()
        if fileName is None: fileName = self._autodetermineFileName()
        self.filePath = folderPath / fileName

    def __repr__(self):
        return f'GoldenSampleReporter for component "{self._component.name}" at path "{self.filePath}".'

    @property
    def filePath(self) -> Path:
        return self._filePath
    
    @filePath.setter
    def filePath(self, newPath:Path):
        if not isinstance(newPath, Path):
            raise TypeError(f'filePath must be a pathlib Path object (it is {type(newPath)}).')
        if not newPath.suffix == '.out':
            raise ValueError(f'File path must have a ".out" extension.')
        
        self._filePath = newPath

    def _autodetermineFolderPath(self) -> Path:
        benchConfig = getBenchConfig(hostname())
        pathString = Path(benchConfig.get('goldenSampleFolderPath'))
        if pathString is None:
            raise MissingInformation(f'Could not retrieve "goldenSampleFolderPath" for machine {hostname()}".')
        return pathString
    
    def _autodetermineFileName(self) -> str:
        return 'GS_' + self._component.name + '.out'

    def generateReport(self) -> DataFrame:
        return DataFrame(_generateCompleteReport(self._connection, self._component))

    def saveAllData(self):
        """Saves the complete report to a CSV file in the specified path.
        
        Raises FileExistsError if the file already exists."""

        if self._filePath.exists():
            raise FileExistsError(f'File "{self._filePath}" already exists. Cannot overwrite.')
        
        report = _generateCompleteReport(self._connection, self._component)
        _saveReportToCSV(report, self._filePath)
        log.info(f'Saved all data for golden sample "{self._component.name}" to file {self._filePath}.')

    def appendLastMeasurement(self):
        """Appends the last measurement to the report file.
        
        If the file does not exist, it is created."""
        
        report = _generateCompleteReport(self._connection, self._component)
        lastLine = report[-1]
        
        _appendReportLine(lastLine, self._filePath)
        log.info(f'Appended line for golden sample "{self._component.name}" to file {self._filePath}.')