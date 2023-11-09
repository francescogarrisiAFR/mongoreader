from mongomanager import log
from mongomanager import component as cmpClass
from mongomanager import blueprint
from datautils import dataClass

class chipGoggleFunctions:

    def __init__(self):
        pass

    @staticmethod
    def chipStatus(chip):
        return chip.status


    # --------------------------------------------------------------------------
    # Test history scoop functions

    def scoopResultsFromTestEntry(
            testEntry:dict,
            searchResultName_orNames,
            locationNames:str = None,
            searchDatasheetReady:bool = True,
            requiredStatus_orList = None,
            requiredProcessStage_orList = None,
            requiredTags:list = None,
            tagsToExclude:list = None,
            ) -> list:
        """Given an entry of a test history of a component ("testEntry"),
        returns a list of dictionaries in the form:
        
        [
            {"result": <resultName>, "data": <data>, "location": <location>},
            {"result": <resultName>, "data": <data>, "location": <location>}
            ...
        ]

        Additional arguments specify which results are collected, as described
        below.

        Args:
            testHistory (list): The test history of a component to be scooped.
            searchResultName_orNames (str | list[str]): The name(s) of the
                result(s) to be scooped. 
            locationNames (str, optional): If passed, the location(s) where the
                results may be found. If None, results will be scooped without
                checking the location field of the entry.
            searchDatasheetReady (bool, optional): If True, only results that
                have the field "datasheetReady" set to True are scooped.
                If "datasheetReady" is False or missing, the result is ignored.
                Defaults to True.
            requiredStatus_orList (str | list[str], optional): If passed, only
                test entries whose "status" field is equal to requiredStatus are
                considered. If a list is passed, all the elements are considered
                a valid status requirement. Defaults to None, in which case
                "status" is ignored.
            requiredProcessStage_orList (str | list[str], optional): If passed,
                only test entries whose "processStage" field is equal to
                requiredStatus are considered. If a list is passed, all the
                elements are considered a valid status requirement. Defaults to
                None, in which case "processStage" is ignored.
            requiredTags (list[str], optional): If passed, results which lack
                the tags listed here are ignored. Defaults to None.
            tagsToExclude (list[str], optional): If passed, results that have
                tags listed here are ignored. Defaults to None.

        Returns:
            List[dict]: The output described above.
        """

        if isinstance(searchResultName_orNames, list):
            for name in searchResultName_orNames:
                if not isinstance(name, str):
                    raise TypeError('"searchResultName_orNames" must be a string or a list of strings.')
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
                    raise TypeError('"locationNames" must be a list of string.')

        if requiredStatus_orList is None:
            requiredStati = None
        else:
            if isinstance(requiredStatus_orList, str):
                requiredStati = [requiredStatus_orList]
            else:
                if not isinstance(requiredStatus_orList, list):
                    raise TypeError('"requiredStatus_orList" must be a string, a list of strings, or None.')    
                for el in requiredStatus_orList:
                    if not isinstance(el, str):
                        raise TypeError('"requiredStatus_orList" must be a string, a list of strings, or None.')   
                requiredStati = requiredStatus_orList

        if requiredProcessStage_orList is None:
            requiredStages = None
        else:
            if isinstance(requiredProcessStage_orList, str):
                requiredStages = [requiredProcessStage_orList]
            else:
                if not isinstance(requiredProcessStage_orList, list):
                    raise TypeError('"requiredProcessStage_orList" must be a string, a list of strings, or None.')    
                for el in requiredProcessStage_orList:
                    if not isinstance(el, str):
                        raise TypeError('"requiredProcessStage_orList" must be a string, a list of strings, or None.')    
                requiredStages = requiredProcessStage_orList
            
        if requiredProcessStage_orList is not None:
            if not isinstance(requiredProcessStage_orList, str):
                raise TypeError('"requiredProcessStage_orList" must be a string or None.')
            
        if requiredTags is not None:
            if not isinstance(requiredTags, list):
                raise TypeError('"requiredTags" must be a list of strings or None.')
            for el in requiredTags:
                if not isinstance(el, str):
                    raise TypeError('"requiredTags" must be a list of strings or None.')
                
        if tagsToExclude is not None:
            if not isinstance(tagsToExclude, list):
                raise TypeError('"tagsToExclude" must be a list of strings or None.')
            for el in tagsToExclude:
                if not isinstance(el, str):
                    raise TypeError('"tagsToExclude" must be a list of strings or None.')
                        

        found = []

        # Searching general entry fields

        if requiredStati is not None:
            if testEntry.get('status') not in requiredStati:
                return None
        
        if requiredStages is not None:
            if testEntry.get('processStage') not in requiredStages:
                return None

        # Search individual result dictionaries in entry

        entryResults = testEntry.get('results', [])
        for res in entryResults:    

            if searchDatasheetReady:
                # Does not skip if datasheetReady is True.
                # Skip if datasheetReady is False or is missing.
                dready = res.get('datasheetReady')
                if dready is None or dready is False: 
                    continue

            resName = res.get('resultName')
            if resName not in resultNames:
                continue

            loc = res.get('location')
            if locationNames is not None:
                if loc is None or loc not in locationNames:
                    continue

            if requiredTags is not None:
                resTags = res.get('requiredTags')
                if resTags is None or resTags == []:
                    continue

                if not all([tag in resTags for tag in requiredTags]):
                    continue

            if tagsToExclude is not None:
                resTags = res.get('requiredTags')
                if not(resTags is None or resTags == []): # There may be tags to exclude
                    if any([tag in tagsToExclude for tag in resTags]):
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
            searchDatasheetReady:bool = True,
            requiredStatus_orList:str = None,
            requiredProcessStage_orList:str = None,
            requiredTags:list = None,
            tagsToExclude:list = None) -> dict:
        """This function scoops the test history of a component to return a 
        dataDict dictionary or a dictionary of dataDict dictionaries (depending
        on wether "resultName_orNames" is a single string or a list of strings.

        Args:
            testHistory (list): The test history of a component to be scooped.
            resultName_orNames (str | list[str]): The name(s) of the result(s)
                to be scooped. 
            locationNames (str): The location(s) where the results may be found.
            searchDatasheetReady (bool, optional): If True, only results that
                have the field "datasheetReady" set to True are scooped.
                If "datasheetReady" is False or missing, the result is ignored.
                Defaults to True.
            requiredStatus_orList (str | list[str], optional): If passed, only
                test entries whose "status" field is equal to requiredStatus are
                considered. If a list is passed, all the elements are considered
                a valid status requirement. Defaults to None, in which case
                "status" is ignored.
            requiredProcessStage_orList (str | list[str], optional): If passed,
                only test entries whose "processStage" field is equal to
                requiredStatus are considered. If a list is passed, all the
                elements are considered a valid status requirement. Defaults to
                None, in which case "processStage" is ignored.
            requiredTags (list[str], optional): If passed, results which lack
                the tags listed here are ignored. Defaults to None.
            tagsToExclude (list[str], optional): If passed, results that have
                tags listed here are ignored. Defaults to None.

        Returns:
            dict: The dataDict dictionary of scooped results.
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
                            resultName_orNames,
                            locationNames,
                            searchDatasheetReady,
                            requiredStatus_orList,
                            requiredProcessStage_orList,
                            requiredTags,
                            tagsToExclude)

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
    def scoopComponentResults(cls, component, resultName_orNames,
                                locationNames:str,
                                searchDatasheetReady:bool = True,
                                requiredStatus_orList:str = None,
                                requiredProcessStage_orList:str = None,
                                requiredTags:list = None,
                                tagsToExclude:list = None):
        """This function scoops the test history of a component to return a 
        dataDict dictionary or a dictionary of dataDict dictionaries (depending
        on wether "resultName_orNames" is a single string or a list of strings.

        Args:
            component (mongomanager.component): The component whose history is
                to be scooped.
            resultName_orNames (str | list[str]): The name(s) of the result(s)
                to be scooped. 
            locationNames (str): The location(s) where the results may be found.
            searchDatasheetReady (bool, optional): If True, only results that
                have the field "datasheetReady" set to True are scooped.
                If "datasheetReady" is False or missing, the result is ignored.
                Defaults to True.
            requiredStatus_orList (str | list[str], optional): If passed, only
                test entries whose "status" field is equal to requiredStatus are
                considered. If a list is passed, all the elements are considered
                a valid status requirement. Defaults to None, in which case
                "status" is ignored.
            requiredProcessStage_orList (str | list[str], optional): If passed,
                only test entries whose "processStage" field is equal to
                requiredStatus are considered. If a list is passed, all the
                elements are considered a valid status requirement. Defaults to
                None, in which case "processStage" is ignored.
            requiredTags (list[str], optional): If passed, results which lack
                the tags listed here are ignored. Defaults to None.
            tagsToExclude (list[str], optional): If passed, results that have
                tags listed here are ignored. Defaults to None.

        Returns:
            dict: The dataDict dictionary of scooped results."""
        
        if not isinstance(component, cmpClass):
            raise TypeError('scoopComponentReults can only be applied to mongomanager component documents.')

        history = component.getField('testHistory',
                                valueIfNotFound = [],
                                notFoundValues = [[], None],
                                verbose = False)
        
        scoopedDict = cls.scoopResultsFromHistory(history,
            resultName_orNames, locationNames, searchDatasheetReady,
            requiredStatus_orList, requiredProcessStage_orList, requiredTags,
            tagsToExclude)

        if isinstance(resultName_orNames, str):
            return scoopedDict[resultName_orNames]
        else:
            return scoopedDict
    