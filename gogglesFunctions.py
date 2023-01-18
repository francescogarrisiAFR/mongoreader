from mongomanager import log
from datautils import dataClass

class chipGoggleFunctions:

    def __init__(self):
        pass

    @staticmethod
    def chipStatus(chip):
        return chip.status


    def scoopResultsFromTestEntry(
            testEntry:dict,
            searchResultName_orNames,
            locationNames:str = None,
            searchDatasheetReady:bool = True) -> list:
        """Returns a list of dictionaries in the form:
        
        [
            {"result": <resultName>, "data": <data>, "location": <location>},
            {"result": <resultName>, "data": <data>, "location": <location>}
            ...
        ]

        If "locationNames" is None, results will be scooped without checking
        the location field of the test entry.

        If "searchDatasheetReady" is True, the result is collected only if
        the value of the field "datasheedReady" is True.
        """

        if isinstance(searchResultName_orNames, list):
            for name in searchResultName_orNames:
                if not isinstance(name, str):
                    raise ValueError('"searchResultName_orNames" must be a string or a list of strings.')
            resultNames = searchResultName_orNames
        else:
            if not isinstance(searchResultName_orNames, str):
                raise TypeError('"searchResultName_orNames" must be a string or a list of strings.')
            resultNames = [searchResultName_orNames]

        if locationNames is not None:
            if not isinstance(locationNames, list):
                raise TypeError('"locationNames" must be a list of string.')

            for name in locationNames:
                if not isinstance(name, str):
                    raise ValueError('"locationNames" must be a list of string.')

        found = []

        entryResults = testEntry.get('results', [])
        for res in entryResults:    

            if searchDatasheetReady:
                if res.get('datasheetReady') is False:
                    continue

            resName = res.get('resultName')
            if resName not in resultNames:
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
                else:
                    log.spare(f'Found dictionary (result name: "{resName}") is not a valid dataClass dictionary.')

            if data is not None:
                found.append(
                    {
                        'result': resName,
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
            resultName_orNames, 
            locationNames:str,
            searchDatasheetReady:bool = True) -> dict:
        """This function scoops the test history of a component to return a 
        dataDict dictionary or a dictionary of dataDict dictionaries (depending
        on wether "resultName_orNames" is a single string or a list of strings.

        Each dataDict is in the form:
        {
            
        }

        """

        # allScooped is a list of lists of the form
        #   [   
        #       [
        #           {"result": <resultName>, "data": <data>, "location": <location>},
        #           {"result": <resultName>, "data": <data>, "location": <location>}
        #           ...
        #       ],
        #       [
        #           {"result": <resultName>, "data": <data>, "location": <location>},
        #           {"result": <resultName>, "data": <data>, "location": <location>}
        #           ...
        #       ]
        #   ]
        # Each element of the list corresponds to the results scooped from one
        # test history entry.
        # Elements of the list can be equal to None if no results are found.
        # Moreover, location can be None if the result is associated to the
        # whole chip and not to a particular location (e.g. the average IL)

        # Arguments checks

        if locationNames is None:
            raise TypeError('"locationNames" must be a list of strings.')
        for name in locationNames:
            if not isinstance(name, str):
                raise ValueError('"locationNames" must be a list of strings.')


        # Scooping results from all the test history entries.

        allScooped = [cls.scoopResultsFromTestEntry(entry,
            resultName_orNames, locationNames, searchDatasheetReady)
            for entry in reversed(testHistory)] # Most recent is scooped first

        while None in allScooped: allScooped.remove(None)

        if isinstance(resultName_orNames, str):
            resultNames = [resultName_orNames]
        else:
            resultNames = resultName_orNames

        # Instanciating return dictionary
        returnDict = {resName: {locName: None for locName in locationNames}
                        for resName in resultNames}

        for scooped in allScooped: # Scooped is a list of dictionaries
            for dic in scooped:

                loc = dic.get('location')

                if loc is None:
                    # Nothing to do as I would not be able to assign the 
                    # result to the return dictionary
                    continue

                resultName = dic.get('result')
                if resultName is None:
                    # Nothing to do as I would not be able to assign the 
                    # result to the return dictionary
                    continue

                # Checking if returnDict for the given location/resultName pair
                # is not already occupied
                if returnDict[resultName][loc] is not None:
                    continue    # Data was already found

                # Else, I collect data and assign it to the 
                data = dic.get('data')
                
                if data is None:
                    continue # Nothing to do

                returnDict[resultName][loc] = data

        return returnDict


    @classmethod
    def datasheedData(cls, chip,
            resultName_orNames,
            locationNames:list):
        """
        This function is used to search the test history of "chip" for results
        of tests. It searches results named after "resultName_orNames".

        If "resultName_orNames" is a string, the functions returns a dictionary
        in the form
        {
            <location1>: <data1>,
            <location2>: <data2>,
            ...
        }
        corresponding to the data associated to the result and where the keys
        <locationN> are the elements of "locationNames".

        If "resultName_orNames" is a list of strings, the function does the
        same for each element of the list, and results are stored in a list
        of dictionaries in the same form as the one described above.
        """

        history = chip.getField('testHistory', valueIfNotFound = [], verbose = False)
        scoopedDict = cls.scoopResultsFromHistory(history,
            resultName_orNames, locationNames, searchDatasheetReady=True)                       

        if isinstance(resultName_orNames, str):
            return scoopedDict[resultName_orNames]
        else:
            return scoopedDict
    