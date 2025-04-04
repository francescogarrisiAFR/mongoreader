"""This module contains utilities to interact with components' Datasheets
on a collective manner
"""

from pandas import DataFrame, concat
import mongomanager as mom

class _attributeClass:
    def __init__(self, obj):
        self._obj = obj

    @classmethod
    def _attributeName(cls):
        return cls.__name__.lstrip('_')



def _attributeClassDecoratorMaker(attributeClass):

    assert issubclass(attributeClass, _attributeClass), '[_classAttributeDecorator] Not an _attributeClass.'

    def decorator(objClass):

        oldInit = getattr(objClass, '__init__')

        def newInit(self, *args, **kwargs):
            oldInit(self, *args, **kwargs)
            setattr(self, attributeClass._attributeName(), attributeClass(self))

        setattr(objClass, '__init__', newInit)
        
        return objClass

    return decorator



class _DatasheetsBaseClass(_attributeClass):
    """Attribute class to apply Datasheet methods to wafer collations"""
    
    def retrieveData(self,
                    components:list,
                    resultNames:list = None,
                    requiredTags:list = None,
                    tagsToExclude:list = None,
                    locations:list = None,
                    *,
                    returnDataFrame:bool = False,
                    datasheetIndex:int = None,
                    includeWaferLabels:bool = False
                ):
        """This method can be used to retrieve data from datasheets defined
        for the components of the wafer collation.

        The argumetns can be used to change what results are collected, as
        described below.

        Args:
            components (list[mom.component]): 
            resultNames (list, optional): If passed, results whose name is not
                listed here are ignored.
            requiredTags (list, optional): If passed, results tags must contain
                those listed here to be collected.
            tagsToExclude (list, optional): If passed, results whose tags are
                among these are not collected. Defaults to None.
            locations (list, optional): If passed, the result location must be
                among these for it to be collected. Defaults to None.

        Keyword arguments (**kwargs):
            returnDataFrame (bool, optional): If True, results are returned
                as a pandas DataFrame instead of a list of dictionaries.
                Defaults to False.
            datasheetIndex (int, optional): If passed, the datasheet indexed
                by datasheetIndex is passed. See mongomanager.component for
                more info. Defaults to None.
            includeWaferLabels:

        Returns:
            List[dict] | pandas.DataFrame: The collected results.
        """        
        
        allResults = []
        for cmp in components:

            scoopedResults = cmp.Datasheet.retrieveData(
                        resultNames,
                        requiredTags,
                        tagsToExclude,
                        locations = locations,
                        returnDataFrame = False,
                        datasheetIndex = datasheetIndex,
                        verbose = False
                    )
            
            if scoopedResults is None:
                continue

            if includeWaferLabels:
                label = cmp.getField('_waferLabel', verbose = False)
                scoopedResults = [{**{'label': label}, **s} for s in scoopedResults]

            allResults.append(scoopedResults)
            
        # Returning
            
        allResults = _joinListsOrNone(*allResults)

        if returnDataFrame:
            return DataFrame(allResults)

        return allResults




def _joinListsOrNone(*args):
    """Returns a single dictionary from a arguments that can be lists or None."""
    returnList = []

    for arg in args:
        if arg is None:
            continue
        elif not isinstance(arg, list):
            raise TypeError('All arguments must be lists or None.')
        else:
            returnList += arg

    return returnList

def _joinDictsOrNone(*args):
    """Returns a single dictionary from a arguments that can be dictionaries or None."""
    returnDict = {}

    for arg in args:
        if arg is None:
            continue
        elif not isinstance(arg, dict):
            raise TypeError('All arguments must be dictionaries or None.')
        else:
            returnDict |= arg

    return returnDict