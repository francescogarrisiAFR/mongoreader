from mongomanager import log
from datautils import dataClass

class chipGoggleFunctions:

    def __init__(self):
        pass

    def scoopResultsFromTestEntry(
            testEntry:dict,
            resultName:str,
            locationKey:str = None,
            searchDatasheetReady:bool = True) -> list:

        found = []

        results = testEntry.get('results', [])
        for res in results:    

            if searchDatasheetReady:
                if res.get('datasheetReady') is False:
                    continue

            if res.get('resultName') != resultName:
                continue

            loc = res.get('location')
            if locationKey is not None:
                if loc is None or locationKey not in loc:
                    continue

            # If I arrived here, I found the right result

            data = res.get('resultData')
            
            if isinstance(data, dict):
                if dataClass.isDictionaryValid(data):
                    data = data['value']

            if data is not None:            
                found.append(
                    {
                        'data': data,
                        'location': loc,
                    }
                )

        if found == []:
            return None
        else:
            return found

    @classmethod
    def scoopResultsFromHistory(cls,
            testHistory:list,
            resultName:str, 
            locationKey:str,
            searchDatasheetReady:bool = True) -> dict:

        # allScooped is a list of list of dicts of the form
        #   {'data': <data>, 'location': <location>}
        allScooped = [cls.scoopResultsFromTestEntry(entry,
            resultName, locationKey, searchDatasheetReady)
            for entry in reversed(testHistory)] # Most recent is scooped first

        while None in allScooped: allScooped.remove(None)

        scoopedDict = {}

        for scooped in allScooped:
            for dic in scooped:
                loc = dic['location']
                if loc not in scoopedDict:
                    scoopedDict[loc] = dic['data']
                else:
                    pass # No data is added since it was already found
        
        return scoopedDict

    @staticmethod
    def chipStatus(chip):
        return chip.status

    @classmethod
    def datasheedData(cls, chip,
            resultName:str,
            locationKey:str,
            locationsNumber:int):
    
        if not isinstance(resultName, str):
            raise TypeError('"measureName" must be a string.')

        if not isinstance(locationKey, str):
            raise TypeError('"locationKey" must be a string.')

        if not isinstance(locationsNumber, int):
            raise TypeError('"locationNumber" must be a positive integer')

        if locationsNumber <= 0:
            raise ValueError('"locationNumber" must be a positive integer')

        expectedKeys = [f'{locationKey}{n+1}' for n in range(locationsNumber)]

        history = chip.getField('testHistory', valueIfNotFound = [], verbose = False)
        scoopedDict = cls.scoopResultsFromHistory(history,
            resultName, locationKey, searchDatasheetReady=True)

        dataDict = {key: scoopedDict.get(key) for key in expectedKeys}                        

        return dataDict
    