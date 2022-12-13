from mongomanager import log
from datautils import dataClass

class chipGoggleFunctions:

    def __init__(self):
        pass

    def scoopResultsFromTestEntry(
            testEntry:dict,
            resultName:str,
            locationNames:str = None,
            searchDatasheetReady:bool = True) -> list:

        if not isinstance(resultName, str):
            raise TypeError('"resultName" must be a string.')

        if locationNames is not None:
            if not isinstance(locationNames, list):
                raise TypeError('"locationNames" must be a list of string.')

            for name in locationNames:
                if not isinstance(name, str):
                    raise ValueError('"locationNames" must be a list of string.')

        found = []

        results = testEntry.get('results', [])
        for res in results:    

            if searchDatasheetReady:
                if res.get('datasheetReady') is False:
                    continue

            if res.get('resultName') != resultName:
                continue

            loc = res.get('location')
            if locationNames is not None:
                if loc is None or loc not in locationNames:
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
            resultName:list, 
            locationNames:str,
            searchDatasheetReady:bool = True) -> dict:

        # allScooped is a list of list of dicts of the form
        #   {'data': <data>, 'location': <location>}
        allScooped = [cls.scoopResultsFromTestEntry(entry,
            resultName, locationNames, searchDatasheetReady)
            for entry in reversed(testHistory)] # Most recent is scooped first

        while None in allScooped: allScooped.remove(None)

        scoopedDict = {name: None for name in locationNames}

        for scooped in allScooped:
            for dic in scooped:
                loc = dic['location']
                data = dic['data']

                if data is not None and scoopedDict[loc] is None:
                    scoopedDict[loc] = data
                else:
                    pass # No data is added since it was already found

        return scoopedDict

    @staticmethod
    def chipStatus(chip):
        return chip.status

    @classmethod
    def datasheedData(cls, chip,
            resultName:str,
            locationNames:list):
    
        history = chip.getField('testHistory', valueIfNotFound = [], verbose = False)
        scoopedDict = cls.scoopResultsFromHistory(history,
            resultName, locationNames, searchDatasheetReady=True)                       

        return scoopedDict
    