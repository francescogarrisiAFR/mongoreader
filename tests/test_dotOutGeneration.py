import unittest
import mongomanager as mom
from mongoreader.connectors.MMSconnector import (
    DotOutManager,
    DotOutManager_Chips,
    DotOutManager_Modules,
    dotOutManager_MixedStagesCordobaChips
)
from datautils import awareDatetime
from bson import ObjectId
from pathlib import Path
import csv
from datetime import datetime

FOLDER = Path(__file__).parent
SUBFOLDER = FOLDER / '.test-tmp'

# ------------------------------------------------------------------------------
# MOCKUP DATA

LOCATIONS_DICT = {
    'MZ': ['MZ1', 'MZ2', 'MZ3', 'MZ4'],
    'PD': ['PD1', 'PD2', 'PD3', 'PD4'],
    'PS': ['PS1', 'PS2', 'PS3', 'PS4', 'PS5', 'PS6', 'PS7', 'PS8'],
}

DatasheetDefinition = [
        
        {
            'resultName': 'Vpi',
            'valueRange': {
                'min': 2.0,
                'max': 3.0,
                'typical': 2.5,
                'unit': 'V'
            },
            'outliersRange': {
                'min': 0.0,
                'max': 10.0,
                'unit': 'V'
            },
            'locationGroup': 'MZ',
            'datasheetReady': False,
            'selectionMethod': 'bestValue',
            # tagFilters not defined
        },

        {
            'resultName': 'ER',
            'valueRange': {
                'max': None,
                'min': 30.0,
                'unit': 'dB'
            },
            'outliersRange': {
                'min': 0.0,
                'max': 100.0,
                'unit': 'dB'
            },
            'locationGroup': 'PS',
            'datasheetReady': False,
            'selectionMethod': 'max',
            # tagFilters not defined
        },
        
        {
            'resultName': 'IL',
            'valueRange': {
                'min': 0.0,
                'max': 3.0,
                'typical': 2.5,
                'unit': 'dB'
            },
            'outliersRange': {
                'max': 50.0,
                'min': 0.0,
                'unit': 'dB'
            },
            'locationGroup': 'MZ',
            'datasheetReady': False,
            'selectionMethod': 'min',
            # tagFilters not defined
        },

        # To test "rootMeasurement"
        {
            'resultName': 'Responsivity',
            'valueRange': {
                'typical': 0.9,
                'unit': 'A/W'
            },
            'outliersRange': {
                'max': 1.2,
                'min': 0.0,
                'unit': 'A/W'
            },
            'locationGroup': 'PD',
            'datasheetReady': False,
            'selectionMethod': 'rootMeasurement',
            'methodInfo': {
                'rootMeasurement': 'IL',
                'rootLocationsMap': {'PD1': 'MZ1', 'PD2': 'MZ2', 'PD3': 'MZ3', 'PD4': 'MZ4'},
                'rootRequiredTags': None,
                'rootTagsToExclude': None,
                'backupMethod': 'max',
            },
            # tagFilters not defined
        },
    ]

FAKE_IDS = [
    ObjectId('65e98244831c5256e1d9ce0b'),
    ObjectId('65e98244831c5256e1d9ce0c'),
    ObjectId('65e98244831c5256e1d9ce0d'),
    ObjectId('65e98244831c5256e1d9ce0e'),
    ObjectId('65e98244831c5256e1d9ce0f'),
    ObjectId('65e98244831c5256e1d9ce10'),
    ObjectId('65e98244831c5256e1d9ce11'),
    ObjectId('65e98244831c5256e1d9ce12'),
    ObjectId('65e98244831c5256e1d9ce13'),
    ObjectId('65e98244831c5256e1d9ce14'),
    ObjectId('65e98244831c5256e1d9ce15'),
    ObjectId('65e98244831c5256e1d9ce16'),
    ObjectId('65e98244831c5256e1d9ce17'),
    ObjectId('65e98244831c5256e1d9ce18'),
    ObjectId('65e98244831c5256e1d9ce19'),
]

dataset1 = {
    'header': ['MZ1', 'MZ2', 'MZ3', 'MZ4', 'PD1', 'PD2', 'PD3', 'PD4'],
    'measurementNames': ['IL and Responsivity 1', 'IL and Responsivity 2', 'IL and Responsivity 3', 'IL and Responsivity 4'],
    'measurementIndexes': [0,1,2,3],
    'measurementStageStatus': [(1,1), (1,2), (2,1), (2,3)],
    'measurementDates': [awareDatetime('Europe/Rome', 2021, 1, 1), awareDatetime('Europe/Rome', 2021, 1, 2), awareDatetime('Europe/Rome', 2021, 1, 3), awareDatetime('Europe/Rome', 2021, 1, 4)],
    'resultsNames': ['IL', 'IL', 'IL', 'IL', 'Responsivity', 'Responsivity', 'Responsivity', 'Responsivity'],
    'resultsData': [
        [2.4,2.45,3.4,None, 0.7,0.7,None,0.7],
        [2.3,-0.3,3.45,None, 0.8,0.9,None, 0.8],
        [2.5,2.3,3.8,None, 0.9,0.75,1.5,0.9],
        [2.6,2.4,3.9,None,0.85,None,-0.3,0.85],
    ],
    'resultsUnits': ['dB', 'dB', 'dB', 'dB', 'A/W', 'A/W', 'A/W', 'A/W'],
}

dataset2 = {
    'header': ['MZ1', 'MZ2', 'MZ3', 'MZ4', 'PS1', 'PS2', 'PS3', 'PS4', 'PS5', 'PS6', 'PS7', 'PS8'],
    'measurementNames': ['Vpi and ER 1', 'Vpi and ER 2', 'Vpi and ER 3', 'Vpi and ER 4', 'Vpi and ER 5', 'Vpi and ER 6'],
    'measurementIndexes': [4,5,6,7,8,9,10,11],
    'measurementStageStatus': [(1,1), (1,2), (2,3), (2,3), (2,4), (3,5)],
    'measurementDates': [awareDatetime('Europe/Rome', 2021, 1, 5), awareDatetime('Europe/Rome', 2021, 1, 6), awareDatetime('Europe/Rome', 2021, 1, 7), awareDatetime('Europe/Rome', 2021, 1, 8), awareDatetime('Europe/Rome', 2021, 1, 9), awareDatetime('Europe/Rome', 2021, 1, 10)],
    'resultsNames': ['Vpi', 'Vpi', 'Vpi', 'Vpi', 'ER', 'ER', 'ER', 'ER', 'ER', 'ER', 'ER', 'ER'],
    'resultsData': [
        [2.5,2.45,None,None, None,None,None,None,None,None,None,None],
        [None,None, 2.5,2.55, None,None,None,None,None,None,None,None],
        [2.5,2.7,2.5,None, None,None,None,None,None,None,None,None],
        [None,None,None,None, 35,40,25,-0.5,None,None,None,None],
        [None,None,None,None, 40,45,35,33,None,None,None,None],
        [2.6,2.6,15,2.5,45,40,None,31,-5,20,110,None],
    ],
    'resultsUnits': ['V', 'V', 'V', 'V', 'dB', 'dB', 'dB', 'dB', 'dB', 'dB', 'dB', 'dB'],
}

def generateTestHistoryEntries(dataset):

    locations = dataset['header']
    measurementNames = dataset['measurementNames']
    measurementIndexes = dataset['measurementIndexes']
    stages, stati = zip(*dataset['measurementStageStatus'])
    dates = dataset['measurementDates']

    resultsNames = dataset['resultsNames']
    resultsData = dataset['resultsData']
    resultsUnits = dataset['resultsUnits']

    history = []

    for entryIndex in range(len(measurementNames)):

        entry = {
            'name': measurementNames[entryIndex],
            'testReportID': FAKE_IDS[measurementIndexes[entryIndex]],
            'rawDataIDs': None,
            'rawData': None,
            'executionDate': dates[entryIndex],
            'status': f'status {stati[entryIndex]}',
            'processStage': f'stage {stages[entryIndex]}',
            'results': []
        }


        for dataIndex in range(len(resultsData[entryIndex])):
            
            resultDict = {
                'resultName': resultsNames[dataIndex],
                'location': locations[dataIndex],
                'resultData': {
                    'value': resultsData[entryIndex][dataIndex],
                    'error': 0.01,
                    'unit': resultsUnits[dataIndex],
                }
                # No result tags
            }

            entry['results'].append(resultDict)
        
        history.append(entry)
    
    return history

MOCKUP_HISTORY = generateTestHistoryEntries(dataset1) + generateTestHistoryEntries(dataset2)

# ------------------------------------------------------------------------------
# EXPECTED DATA

EXPECTED_RECORD_CHIPS_NO_STAGE_STATUS_SELECTION = {
    'DUT_ID': '3CA000059',
    'LOT_ID': '3CA0000',
    'type': 'COR-V3',
    'ChipID': '59',
    'earliestTestDate': awareDatetime('Europe/Rome', 2021, 1, 1),
    'latestTestDate': awareDatetime('Europe/Rome', 2021, 1, 10),
    'bench': None,
    'operator': None,
    'Vpi_MZ1': '2.50',
    'Vpi_MZ2': '2.45',
    'Vpi_MZ3': '2.50',
    'Vpi_MZ4': '2.50',
    'ER_PS1': '45.00',
    'ER_PS2': '45.00',
    'ER_PS3': '35.00',
    'ER_PS4': '33.00',
    'ER_PS5': None,
    'ER_PS6': '20.00',
    'ER_PS7': None,
    'ER_PS8': None,
    'IL_MZ1': '2.30',
    'IL_MZ2': '2.30',
    'IL_MZ3': '3.40',
    'IL_MZ4': None,
    'Responsivity_PD1': '0.80',
    'Responsivity_PD2': '0.75',
    'Responsivity_PD3': None,
    'Responsivity_PD4': '0.90',
}

EXPECTED_RECORD_CHIPS_STAGE1_SELECTION = {
    'DUT_ID': '3CA000059',
    'LOT_ID': '3CA0000',
    'type': 'COR-V3',
    'ChipID': '59',
    'earliestTestDate_stage-1': awareDatetime('Europe/Rome', 2021, 1, 1),
    'latestTestDate_stage-1': awareDatetime('Europe/Rome', 2021, 1, 6),
    'bench_stage-1': None,
    'operator_stage-1': None,
    'Vpi_MZ1_stage-1': '2.50',
    'Vpi_MZ2_stage-1': '2.45',
    'Vpi_MZ3_stage-1': '2.50',
    'Vpi_MZ4_stage-1': '2.55',
    'ER_PS1_stage-1': None,
    'ER_PS2_stage-1': None,
    'ER_PS3_stage-1': None,
    'ER_PS4_stage-1': None,
    'ER_PS5_stage-1': None,
    'ER_PS6_stage-1': None,
    'ER_PS7_stage-1': None,
    'ER_PS8_stage-1': None,
    'IL_MZ1_stage-1': '2.30',
    'IL_MZ2_stage-1': '2.45',
    'IL_MZ3_stage-1': '3.40',
    'IL_MZ4_stage-1': None,
    'Responsivity_PD1_stage-1': '0.80',
    'Responsivity_PD2_stage-1': '0.70',
    'Responsivity_PD3_stage-1': None,
    'Responsivity_PD4_stage-1': '0.80',
}

EXPECTED_RECORD_CHIPS_STAGE2_SELECTION = {
    'DUT_ID': '3CA000059',
    'LOT_ID': '3CA0000',
    'type': 'COR-V3',
    'ChipID': '59',
    'earliestTestDate_stage-2': awareDatetime('Europe/Rome', 2021, 1, 3),
    'latestTestDate_stage-2': awareDatetime('Europe/Rome', 2021, 1, 9),
    'bench_stage-2': None,
    'operator_stage-2': None,
    'Vpi_MZ1_stage-2': '2.50',
    'Vpi_MZ2_stage-2': '2.70',
    'Vpi_MZ3_stage-2': '2.50',
    'Vpi_MZ4_stage-2': None,
    'ER_PS1_stage-2': '40.00',
    'ER_PS2_stage-2': '45.00',
    'ER_PS3_stage-2': '35.00',
    'ER_PS4_stage-2': '33.00',
    'ER_PS5_stage-2': None,
    'ER_PS6_stage-2': None,
    'ER_PS7_stage-2': None,
    'ER_PS8_stage-2': None,
    'IL_MZ1_stage-2': '2.50',
    'IL_MZ2_stage-2': '2.30',
    'IL_MZ3_stage-2': '3.80',
    'IL_MZ4_stage-2': None,
    'Responsivity_PD1_stage-2': '0.90',
    'Responsivity_PD2_stage-2': '0.75',
    'Responsivity_PD3_stage-2': None,
    'Responsivity_PD4_stage-2': '0.90',
}


EXPECTED_RECORD_CHIPS_STAGE3_SELECTION = {
    'DUT_ID': '3CA000059',
    'LOT_ID': '3CA0000',
    'type': 'COR-V3',
    'ChipID': '59',
    'earliestTestDate_stage-3': awareDatetime('Europe/Rome', 2021, 1, 10),
    'latestTestDate_stage-3': awareDatetime('Europe/Rome', 2021, 1, 10),
    'bench_stage-3': None,
    'operator_stage-3': None,
    'Vpi_MZ1_stage-3': '2.60',
    'Vpi_MZ2_stage-3': '2.60',
    'Vpi_MZ3_stage-3': None,
    'Vpi_MZ4_stage-3': '2.50',
    'ER_PS1_stage-3': '45.00',
    'ER_PS2_stage-3': '40.00',
    'ER_PS3_stage-3': None,
    'ER_PS4_stage-3': '31.00',
    'ER_PS5_stage-3': None,
    'ER_PS6_stage-3': '20.00',
    'ER_PS7_stage-3': None,
    'ER_PS8_stage-3': None,
    'IL_MZ1_stage-3': None,
    'IL_MZ2_stage-3': None,
    'IL_MZ3_stage-3': None,
    'IL_MZ4_stage-3': None,
    'Responsivity_PD1_stage-3': None,
    'Responsivity_PD2_stage-3': None,
    'Responsivity_PD3_stage-3': None,
    'Responsivity_PD4_stage-3': None,
}

EXPECTED_RECORD_MIXEDCHIPS_STAGE12_SELECTION = {
    'DUT_ID': '3CA000059',
    'LOT_ID': '3CA0000',
    'type': 'COR-V3',
    'ChipID': '59',
    'earliestTestDate_stage-1_stage-2': awareDatetime('Europe/Rome', 2021, 1, 1),
    'latestTestDate_stage-1_stage-2': awareDatetime('Europe/Rome', 2021, 1, 9),
    'bench_stage-1_stage-2': None,
    'operator_stage-1_stage-2': None,
    'Vpi_MZ1_stage-1_stage-2': '2.50',
    'Vpi_MZ2_stage-1_stage-2': '2.45',
    'Vpi_MZ3_stage-1_stage-2': '2.50',
    'Vpi_MZ4_stage-1_stage-2': '2.55',
    'ER_PS1_stage-1_stage-2': '40.00',
    'ER_PS2_stage-1_stage-2': '45.00',
    'ER_PS3_stage-1_stage-2': '35.00',
    'ER_PS4_stage-1_stage-2': '33.00',
    'ER_PS5_stage-1_stage-2': None,
    'ER_PS6_stage-1_stage-2': None,
    'ER_PS7_stage-1_stage-2': None,
    'ER_PS8_stage-1_stage-2': None,
    'IL_MZ1_stage-1_stage-2': '2.30',
    'IL_MZ2_stage-1_stage-2': '2.30',
    'IL_MZ3_stage-1_stage-2': '3.40',
    'IL_MZ4_stage-1_stage-2': None,
    'Responsivity_PD1_stage-1_stage-2': '0.80',
    'Responsivity_PD2_stage-1_stage-2': '0.75',
    'Responsivity_PD3_stage-1_stage-2': None,
    'Responsivity_PD4_stage-1_stage-2': '0.90',
}

EXPECTED_RECORD_MIXEDCHIPS_STAGE13_SELECTION = {
    'DUT_ID': '3CA000059',
    'LOT_ID': '3CA0000',
    'type': 'COR-V3',
    'ChipID': '59',
    'earliestTestDate_stage-1_stage-3': awareDatetime('Europe/Rome', 2021, 1, 1),
    'latestTestDate_stage-1_stage-3': awareDatetime('Europe/Rome', 2021, 1, 10),
    'bench_stage-1_stage-3': None,
    'operator_stage-1_stage-3': None,
    'Vpi_MZ1_stage-1_stage-3': '2.50',
    'Vpi_MZ2_stage-1_stage-3': '2.45',
    'Vpi_MZ3_stage-1_stage-3': '2.50',
    'Vpi_MZ4_stage-1_stage-3': '2.50',
    'ER_PS1_stage-1_stage-3': '45.00',
    'ER_PS2_stage-1_stage-3': '40.00',
    'ER_PS3_stage-1_stage-3': None,
    'ER_PS4_stage-1_stage-3': '31.00',
    'ER_PS5_stage-1_stage-3': None,
    'ER_PS6_stage-1_stage-3': '20.00',
    'ER_PS7_stage-1_stage-3': None,
    'ER_PS8_stage-1_stage-3': None,
    'IL_MZ1_stage-1_stage-3': '2.30',
    'IL_MZ2_stage-1_stage-3': '2.45',
    'IL_MZ3_stage-1_stage-3': '3.40',
    'IL_MZ4_stage-1_stage-3': None,
    'Responsivity_PD1_stage-1_stage-3': '0.80',
    'Responsivity_PD2_stage-1_stage-3': '0.70',
    'Responsivity_PD3_stage-1_stage-3': None,
    'Responsivity_PD4_stage-1_stage-3': '0.80',
}

EXPECTED_RECORD_MIXEDCHIPS_STAGE23_SELECTION = {
    'DUT_ID': '3CA000059',
    'LOT_ID': '3CA0000',
    'type': 'COR-V3',
    'ChipID': '59',
    'earliestTestDate_stage-2_stage-3': awareDatetime('Europe/Rome', 2021, 1, 3),
    'latestTestDate_stage-2_stage-3': awareDatetime('Europe/Rome', 2021, 1, 10),
    'bench_stage-2_stage-3': None,
    'operator_stage-2_stage-3': None,
    'Vpi_MZ1_stage-2_stage-3': '2.50',
    'Vpi_MZ2_stage-2_stage-3': '2.60',
    'Vpi_MZ3_stage-2_stage-3': '2.50',
    'Vpi_MZ4_stage-2_stage-3': '2.50',
    'ER_PS1_stage-2_stage-3': '45.00',
    'ER_PS2_stage-2_stage-3': '45.00',
    'ER_PS3_stage-2_stage-3': '35.00',
    'ER_PS4_stage-2_stage-3': '33.00',
    'ER_PS5_stage-2_stage-3': None,
    'ER_PS6_stage-2_stage-3': '20.00',
    'ER_PS7_stage-2_stage-3': None,
    'ER_PS8_stage-2_stage-3': None,
    'IL_MZ1_stage-2_stage-3': '2.50',
    'IL_MZ2_stage-2_stage-3': '2.30',
    'IL_MZ3_stage-2_stage-3': '3.80',
    'IL_MZ4_stage-2_stage-3': None,
    'Responsivity_PD1_stage-2_stage-3': '0.90',
    'Responsivity_PD2_stage-2_stage-3': '0.75',
    'Responsivity_PD3_stage-2_stage-3': None,
    'Responsivity_PD4_stage-2_stage-3': '0.90',
}

EXPECTED_RECORD_MIXEDCHIPS_STAGE123_SELECTION = {
    'DUT_ID': '3CA000059',
    'LOT_ID': '3CA0000',
    'type': 'COR-V3',
    'ChipID': '59',
    'earliestTestDate_stage-1_stage-2_stage-3': awareDatetime('Europe/Rome', 2021, 1, 1),
    'latestTestDate_stage-1_stage-2_stage-3': awareDatetime('Europe/Rome', 2021, 1, 10),
    'bench_stage-1_stage-2_stage-3': None,
    'operator_stage-1_stage-2_stage-3': None,
    'Vpi_MZ1_stage-1_stage-2_stage-3': '2.50',
    'Vpi_MZ2_stage-1_stage-2_stage-3': '2.45',
    'Vpi_MZ3_stage-1_stage-2_stage-3': '2.50',
    'Vpi_MZ4_stage-1_stage-2_stage-3': '2.50',
    'ER_PS1_stage-1_stage-2_stage-3': '45.00',
    'ER_PS2_stage-1_stage-2_stage-3': '45.00',
    'ER_PS3_stage-1_stage-2_stage-3': '35.00',
    'ER_PS4_stage-1_stage-2_stage-3': '33.00',
    'ER_PS5_stage-1_stage-2_stage-3': None,
    'ER_PS6_stage-1_stage-2_stage-3': '20.00',
    'ER_PS7_stage-1_stage-2_stage-3': None,
    'ER_PS8_stage-1_stage-2_stage-3': None,
    'IL_MZ1_stage-1_stage-2_stage-3': '2.30',
    'IL_MZ2_stage-1_stage-2_stage-3': '2.30',
    'IL_MZ3_stage-1_stage-2_stage-3': '3.40',
    'IL_MZ4_stage-1_stage-2_stage-3': None,
    'Responsivity_PD1_stage-1_stage-2_stage-3': '0.80',
    'Responsivity_PD2_stage-1_stage-2_stage-3': '0.75',
    'Responsivity_PD3_stage-1_stage-2_stage-3': None,
    'Responsivity_PD4_stage-1_stage-2_stage-3': '0.90',
}


# ------------------------------------------------------------------------------
# Document generation

def generateOpticalChipBlueprint() -> mom.opticalChipBlueprint:

    bp = mom.opticalChipBlueprint.spawn('test blueprint')
    bp.generateID()

    if not isinstance(bp, mom.opticalChipBlueprint):
        raise TypeError('generateOpticalChipBlueprint() is not spawning an opticalChipBlueprint.')
    
    # Adding locations and location groups

    for locationGroup, locations in LOCATIONS_DICT.items():
        bp.Locations.addElements(locations)
        bp.Locations.addGroup(locationGroup, locations)

    # Adding datasheet defintion
        
    bp.replaceDatasheetDefinition(DatasheetDefinition, verbose = False)

    return bp

def generateChip(blueprint:mom.opticalChipBlueprint) -> mom.opticalChipBlueprint:
    
    if not isinstance(blueprint, mom.opticalChipBlueprint):
        raise TypeError('generateChip() is not receiving a blueprint.')

    cmp = blueprint.spawnEmptyComponent()
    cmp.name = '3CA0000_COR-V3-14'
    cmp.generateID()

    if not isinstance(cmp, mom.component):
        raise TypeError('generateEmptyComponent() is not spawning a component.')

    cmp.setField('testHistory', MOCKUP_HISTORY)

    return cmp

# ------------------------------------------------------------------------------
# Data frame generation

def generateDotOutRecord(manager:DotOutManager, component:mom.component) -> dict|None:

    with mom.logMode(mom.log, 'ERROR'):
        manager._generateDotOutData(component)

        DF = manager._dotOutDF(component)

        if DF is None: return None
        
        record = DF.to_dict(orient='records')[0]

        # Converting dates to aware datetime

        for key, value in record.items():
            if 'earliestTestDate' in key or 'latestTestDate' in key:
                record[key] = value.to_pydatetime()

        return record
    
# ------------------------------------------------------------------------------
# File management
    
class testFileManager:

    def __init__(self, folder:Path = None):

        folder = FOLDER
        assert isinstance(folder, Path)

        self.folder = folder

    @property
    def subFolder(self) -> Path:
        return self.folder / '.test-tmp'

    def ensureSubFolder(self):
        self.subFolder.mkdir(exist_ok=True)
        mom.log.debug(f'[testFileManager.ensureSubFolder] Created subfolder: {self.subFolder}')
    
    def deleteSubFolderContents(self):
        mom.log.debug(f'[testFileManager.deleteSubFolderContents] Deleting contents of subfolder: {self.subFolder}')
        for file in self.subFolder.iterdir():
            file.unlink()
            mom.log.debug(f'[testFileManager.deleteSubFolderContents] Deleted file: {file}')

    def deleteSubFolder(self):
        self.subFolder.rmdir()
        mom.log.debug(f'[testFileManager.deleteSubFolder] Deleted subfolder: {self.subFolder}')

def readDotOutFile(filePath:Path) -> tuple[list[str], list[list[str]]]:

    if not filePath.exists():
        raise FileNotFoundError(f'File not found: {filePath}')
    
    with open(filePath, 'r') as file:
        reader = csv.reader(file)
        lines = list(reader)

    mom.log.debug(f'[readDotOutFile] Read {len(lines)} lines from file: {filePath}')
    for index, line in enumerate(lines):
        mom.log.debug(f'[readDotOutFile]     [{index:3}] >{line}')

    return lines

class dotOutAnalyzer:

    def __init__(self, lines):

        self.header = lines[0]
        self.data = lines[1:] # Including first empty line

    def record(self, index:int) -> dict:
        """index = 0 is the empty line."""

        def normalizeValue(key, val):
            if val == '':
                return None
            if key.startswith('earliestTestDate') or key.startswith('latestTestDate'):
                return datetime.fromisoformat(val)
            return val

        values = self.data[index]

        if len(values) == 0:
            return {} # Empty line

        if len(values) > len(self.header):
            raise ValueError('Record has more values than header.')
               
        record = {k: None for k in self.header}

        for k, v in zip(self.header, values):
            record[k] = normalizeValue(k, v)

        return record
    
    def records(self) -> list[dict]:
        return [self.record(i) for i in range(0, len(self.data))] # Including empty line

# ------------------------------------------------------------------------------


class TestDotOutGeneration(unittest.TestCase):
    
    bp = generateOpticalChipBlueprint()
    chip = generateChip(bp)

    def test_dotOutGeneration_chips_noStageStatusSelection(self):

        man = DotOutManager_Chips(None, FOLDER,
                    blueprint = self.bp,
                    processStage = None,
                    mongoDBupload = False,
                    MMSupload = False,
                    allResultDigits = False,
                    scientificNotationThreshold=10**9)
        
        record = generateDotOutRecord(man, self.chip)
        self.assertEqual(record, EXPECTED_RECORD_CHIPS_NO_STAGE_STATUS_SELECTION)

    def test_dotOutGeneration_chips_stage1(self):

        man = DotOutManager_Chips(None, FOLDER,
                    blueprint = self.bp,
                    processStage = 'stage 1',
                    mongoDBupload = False,
                    MMSupload = False,
                    allResultDigits = False,
                    scientificNotationThreshold=10**9)
        
        record = generateDotOutRecord(man, self.chip)
        self.assertEqual(record, EXPECTED_RECORD_CHIPS_STAGE1_SELECTION)

    def test_dotOutGeneration_chips_stage2(self):

        man = DotOutManager_Chips(None, FOLDER,
                    blueprint = self.bp,
                    processStage = 'stage 2',
                    mongoDBupload = False,
                    MMSupload = False,
                    allResultDigits = False,
                    scientificNotationThreshold=10**9)
        
        record = generateDotOutRecord(man, self.chip)
        self.assertEqual(record, EXPECTED_RECORD_CHIPS_STAGE2_SELECTION)

    def test_dotOutGeneration_chips_stage3(self):

        man = DotOutManager_Chips(None, FOLDER,
                    blueprint = self.bp,
                    processStage = 'stage 3',
                    mongoDBupload = False,
                    MMSupload = False,
                    allResultDigits = False,
                    scientificNotationThreshold=10**9)
        
        record = generateDotOutRecord(man, self.chip)
        self.assertEqual(record, EXPECTED_RECORD_CHIPS_STAGE3_SELECTION)

    def test_dotOutGeneration_mixedChips_stage12(self):

        man = dotOutManager_MixedStagesCordobaChips(None, FOLDER,
                    blueprint = self.bp,
                    processStage_orStages = ['stage 1', 'stage 2'],
                    mongoDBupload = False,
                    MMSupload = False,
                    allResultDigits = False,
                    scientificNotationThreshold=10**9)
        
        record = generateDotOutRecord(man, self.chip)
        self.assertEqual(record, EXPECTED_RECORD_MIXEDCHIPS_STAGE12_SELECTION)

    def test_dotOutGeneration_mixedChips_stage13(self):

        man = dotOutManager_MixedStagesCordobaChips(None, FOLDER,
                    blueprint = self.bp,
                    processStage_orStages = ['stage 1', 'stage 3'],
                    mongoDBupload = False,
                    MMSupload = False,
                    allResultDigits = False,
                    scientificNotationThreshold=10**9)
        
        record = generateDotOutRecord(man, self.chip)
        self.assertEqual(record, EXPECTED_RECORD_MIXEDCHIPS_STAGE13_SELECTION)

    def test_dotOutGeneration_mixedChips_stage23(self):

        man = dotOutManager_MixedStagesCordobaChips(None, FOLDER,
                    blueprint = self.bp,
                    processStage_orStages = ['stage 2', 'stage 3'],
                    mongoDBupload = False,
                    MMSupload = False,
                    allResultDigits = False,
                    scientificNotationThreshold=10**9)
        
        record = generateDotOutRecord(man, self.chip)
        self.assertEqual(record, EXPECTED_RECORD_MIXEDCHIPS_STAGE23_SELECTION)

    def test_dotOutGeneration_mixedChips_stage123(self):

        man = dotOutManager_MixedStagesCordobaChips(None, FOLDER,
                    blueprint = self.bp,
                    processStage_orStages = ['stage 1', 'stage 2', 'stage 3'],
                    mongoDBupload = False,
                    MMSupload = False,
                    allResultDigits = False,
                    scientificNotationThreshold=10**9)
        
        record = generateDotOutRecord(man, self.chip)
        self.assertEqual(record, EXPECTED_RECORD_MIXEDCHIPS_STAGE123_SELECTION)


class test_dotOutFiles(unittest.TestCase):

    def setUp(self):

        self.bp = generateOpticalChipBlueprint()
        self.chip = generateChip(self.bp)

        self.fileManager = testFileManager()
        self.fileManager.ensureSubFolder()
        self.fileManager.deleteSubFolderContents()

        mom.log.errorMode()

    def tearDown(self) -> None:

        self.fileManager.deleteSubFolderContents()
        self.fileManager.deleteSubFolder()

        mom.log.normalMode()

    def test_dotOutFiles_noSelection(self):

        man = DotOutManager_Chips(None, SUBFOLDER,
            blueprint = self.bp,
            processStage = None,
            mongoDBupload = False,
            MMSupload = False,
            allResultDigits = False,
            scientificNotationThreshold=10**9)
        
        filePath = man.saveDotOutLine(self.chip)

        analyzer = dotOutAnalyzer(readDotOutFile(filePath))
        records = analyzer.records()

        self.assertEqual(records[0], {})
        self.assertEqual(records[1], EXPECTED_RECORD_CHIPS_NO_STAGE_STATUS_SELECTION)
        self.assertRaises(IndexError, lambda: records[2])

        man.saveDotOutLine(self.chip) # Another equal line

        analyzer = dotOutAnalyzer(readDotOutFile(filePath))
        records = analyzer.records()

        self.assertEqual(records[0], {})
        self.assertEqual(records[1], EXPECTED_RECORD_CHIPS_NO_STAGE_STATUS_SELECTION)
        self.assertEqual(records[2], EXPECTED_RECORD_CHIPS_NO_STAGE_STATUS_SELECTION)
        self.assertRaises(IndexError, lambda: records[3])

    def test_dotOutFiles_stage1(self):

        man = DotOutManager_Chips(None, SUBFOLDER,
            blueprint = self.bp,
            processStage = 'stage 1',
            mongoDBupload = False,
            MMSupload = False,
            allResultDigits = False,
            scientificNotationThreshold=10**9)
        
        filePath = man.saveDotOutLine(self.chip)

        analyzer = dotOutAnalyzer(readDotOutFile(filePath))
        records = analyzer.records()

        self.assertEqual(records[0], {})
        self.assertEqual(records[1], EXPECTED_RECORD_CHIPS_STAGE1_SELECTION)
        self.assertRaises(IndexError, lambda: records[2])

    def test_dotOutFiles_stage1(self):

        man = DotOutManager_Chips(None, SUBFOLDER,
            blueprint = self.bp,
            processStage = 'stage 2',
            mongoDBupload = False,
            MMSupload = False,
            allResultDigits = False,
            scientificNotationThreshold=10**9)
        
        filePath = man.saveDotOutLine(self.chip)

        analyzer = dotOutAnalyzer(readDotOutFile(filePath))
        records = analyzer.records()

        self.assertEqual(records[0], {})
        self.assertEqual(records[1], EXPECTED_RECORD_CHIPS_STAGE2_SELECTION)
        self.assertRaises(IndexError, lambda: records[2])

    def test_dotOutFiles_stage3(self):

        man = DotOutManager_Chips(None, SUBFOLDER,
            blueprint = self.bp,
            processStage = 'stage 3',
            mongoDBupload = False,
            MMSupload = False,
            allResultDigits = False,
            scientificNotationThreshold=10**9)
        
        filePath = man.saveDotOutLine(self.chip)

        analyzer = dotOutAnalyzer(readDotOutFile(filePath))
        records = analyzer.records()

        self.assertEqual(records[0], {})
        self.assertEqual(records[1], EXPECTED_RECORD_CHIPS_STAGE3_SELECTION)
        self.assertRaises(IndexError, lambda: records[2])

    def test_dotOutFiles_mixedStages12(self):
   
        man = dotOutManager_MixedStagesCordobaChips(None, SUBFOLDER,
            blueprint = self.bp,
            processStage_orStages = ['stage 1', 'stage 2'],
            mongoDBupload = False,
            MMSupload = False,
            allResultDigits = False,
            scientificNotationThreshold=10**9)
        
        filePath = man.saveDotOutLine(self.chip)

        analyzer = dotOutAnalyzer(readDotOutFile(filePath))
        records = analyzer.records()

        self.assertEqual(records[0], {})
        self.assertEqual(records[1], EXPECTED_RECORD_MIXEDCHIPS_STAGE12_SELECTION)
        self.assertRaises(IndexError, lambda: records[2])

    def test_dotOutFiles_mixedStages13(self):
   
        man = dotOutManager_MixedStagesCordobaChips(None, SUBFOLDER,
            blueprint = self.bp,
            processStage_orStages = ['stage 1', 'stage 3'],
            mongoDBupload = False,
            MMSupload = False,
            allResultDigits = False,
            scientificNotationThreshold=10**9)
        
        filePath = man.saveDotOutLine(self.chip)

        analyzer = dotOutAnalyzer(readDotOutFile(filePath))
        records = analyzer.records()

        self.assertEqual(records[0], {})
        self.assertEqual(records[1], EXPECTED_RECORD_MIXEDCHIPS_STAGE13_SELECTION)
        self.assertRaises(IndexError, lambda: records[2])

    def test_dotOutFiles_mixedStages23(self):
   
        man = dotOutManager_MixedStagesCordobaChips(None, SUBFOLDER,
            blueprint = self.bp,
            processStage_orStages = ['stage 2', 'stage 3'],
            mongoDBupload = False,
            MMSupload = False,
            allResultDigits = False,
            scientificNotationThreshold=10**9)
        
        filePath = man.saveDotOutLine(self.chip)

        analyzer = dotOutAnalyzer(readDotOutFile(filePath))
        records = analyzer.records()

        self.assertEqual(records[0], {})
        self.assertEqual(records[1], EXPECTED_RECORD_MIXEDCHIPS_STAGE23_SELECTION)
        self.assertRaises(IndexError, lambda: records[2])


if __name__ == '__main__':
    unittest.main()