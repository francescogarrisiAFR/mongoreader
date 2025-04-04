"""Code found here is used to extract the workload from a given test bench"""

from typing import TypedDict, Literal
from datetime import datetime
from socket import gethostname

from bson import ObjectId

from bson.json_util import default
from datautils.loggers import logger as Logger
from datautils import isDatetimeAware
import mongomanager as mom
from mongomanager import benchTracker, DocumentNotFound, DocumentReference, testingSession, testReport, testReport_v1
from mongomanager.utilsTesting import importBenchTrackerByHostname

class TestingSessionIDLogEntry(TypedDict):
    dateOfChange: datetime
    testingSessionReference: DocumentReference

class WorkloadAnalyzerSettings(TypedDict):
    hostname: str
    loggerLevel: Literal['DEBUG', 'SPARE', 'INFO', 'IMPORTANT', 'WARNING', 'ERROR', 'CRITICAL'] = 'INFO'

class WorkloadAnalyzerInfo(TypedDict):
    startDate: datetime|None
    stopDate: datetime|None
    testingSessionsInfo: list['TestingSessionInfo']

class TestingSessionInfo(TypedDict):
    testingSession: testingSession
    openingDate: datetime|None
    closingDate: datetime|None
    testReportsInfo: list['TestReportInfo']

class TestReportInfo(TypedDict):
    creationDate: datetime
    lastModified: datetime
    executionDate: datetime|None
    executionDuration: float|None
    testReport: testReport|testReport_v1



class WorkloadAnalyzer:

    def __init__(self, settings:WorkloadAnalyzerSettings) -> None:
        
        self.logger = Logger('Test bench WorkloadAnalyzer')
        self.logger.setConsoleLevel(settings['loggerLevel'])

        self._settings = settings
        self.hostname = settings['hostname'].strip().upper()

    def getWorloadInfo(self, conn:mom.connection, startDate:datetime|None = None, stopDate:datetime|None = None) -> WorkloadAnalyzerInfo:

        with mom.opened(conn):

            tsLog = self._getTestingSessionIDsLog(conn)
            tss = self._importAndFilterTestingSessions(conn, tsLog, startDate, stopDate)
            
            info = WorkloadAnalyzerInfo()
            info['startDate'] = startDate
            info['stopDate'] = stopDate
            info['testingSessionsInfo'] = []

            for ts in tss:
                info['testingSessionsInfo'].append(self._getTestingSessionInfo(conn, ts))

        self.logger.info('Workload analysis completed. Found a total of {len(info["testingSessionsInfo"])} entries.')

        return info

    def _getTestingSessionIDsLog(self, conn:mom.connection) -> list[TestingSessionIDLogEntry]:

        btr = importBenchTrackerByHostname(conn, self.hostname)
        if not btr:
            raise DocumentNotFound(f"Could not find a benchTracker for hostname {self.hostname}")
        
        log:list[dict] = btr.getField('_testingSessionIDlog')

        # I convert the values to document references

        self.logger.warning('Currently the test bench log is made of IDs and dates, and IDs are converted to DocumentReferences a posteriori.')

        processedLog:list[TestingSessionIDLogEntry] = []

        for entry in log:

            date = entry.get('dateOfChange')
            tsID = entry.get('value')

            if date is None or (not isinstance(date, datetime)):
                self.logger.warning(f"Invalid dateOfChange entry: {entry} (should be an aware datetime object)")
                continue

            if tsID is None or (not isinstance(tsID, ObjectId)):
                self.logger.warning(f"Invalid value entry: {entry} (should be an ObjectId)")
                continue

            processedLog.append(TestingSessionIDLogEntry({
                'dateOfChange': date,
                'testingSessionReference': DocumentReference(
                    ID = tsID,
                    database = testingSession.defaultDatabase,
                    collection = testingSession.defaultCollection,
                    documentType = 'testingSession',
                )
            }))

        self.logger.info(f'Found a total of {len(processedLog)} entries in the testing session log.')

        return processedLog

    def _importAndFilterTestingSessions(self, conn, tsReferenceLog:list[TestingSessionIDLogEntry], startDate:datetime|None, stopDate:datetime|None) -> list[testingSession]:

        if startDate is not None and not isDatetimeAware(startDate):
            raise TypeError(f"startDate should be None or an aware datetime object (it is {type(startDate)}).")
        if stopDate is not None and not isDatetimeAware(stopDate):
            raise TypeError(f"stopDate should be None or an aware datetime object (it is {type(stopDate)}).")

        with mom.opened(conn):
            allTSs = [entry['testingSessionReference'].retrieveDocument(conn) for entry in tsReferenceLog]

        # I filter the testing sessions by date

        self.logger.debug('Filtering testing sessions by opening and closing date...')

        filteredTSs = []
        for ts in allTSs:

            if startDate is not None:
                opening = ts.getField('sessionOpeningTime', verbose = False)
                if opening and opening < startDate:
                    self.logger.debug(f'Opening date is before the start date ({opening} < {startDate}).')
                    continue

            if stopDate is not None:
                closing = ts.getField('sessionClosingTime', verbose = False)
                if closing and closing > stopDate:
                    self.logger.debug(f'Closing date is before the start date ({closing} > {stopDate}).')
                    continue

            filteredTSs.append(ts)

        self.logger.info(f'Found {len(filteredTSs)} testing sessions in the given date range.')

        return filteredTSs

    def _getTestingSessionInfo(self, conn:mom.connection, ts:testingSession) -> TestingSessionInfo:

        tsInfo = TestingSessionInfo()
        tsInfo['testingSession'] = ts
        tsInfo['openingDate'] = ts.getField('sessionOpeningTime', verbose = False)
        tsInfo['closingDate'] = ts.getField('sessionClosingTime', verbose = False)

        trpsInfos:list[TestReportInfo] = self._getTestReportInfo(conn, ts)
        tsInfo['testReportsInfo'] = trpsInfos

        return tsInfo

    def _getTestReportInfo(self, conn:mom.connection, ts:testingSession) -> list[TestReportInfo]:
        """Returns info on the test reports associated to the given testing session"""
        
        tsID = ts.ID
        info:list[TestReportInfo] = []

        with mom.opened(conn):

            trps = mom.query(conn, {
                'testingSessionID': tsID,
                '$or': [
                    {'type': 'testReport'},
                    {'type': 'testReport_v1'},
                ]
            },
            None, testReport.defaultDatabase, testReport_v1.defaultCollection,
            returnType = 'native',
            verbose = False)

            if trps is None: trps = []

            self.logger.debug(f'Found {len(trps)} reports for testing session {tsID}')

            for trp in trps:
                info.append(TestReportInfo({
                    'creationDate': trp.getField('creationDate', verbose = False),
                    'lastModified': trp.getField('lastModified', verbose = False),
                    'executionDate': trp.getField('executionDate', verbose = False),
                    'executionDuration': trp.getField('executionDuration', verbose = False),
                    'testReport': trp
                }))

        return info