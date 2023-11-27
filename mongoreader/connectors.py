import mongoreader.wafers as w
from datautils import dataClass
from mongomanager.errors import FieldNotFound
from mongomanager import log
from pandas import DataFrame



def datasheetDashboardDFgenerator(connection, waferName:str, *, allResultDigits:bool = False) -> DataFrame:

    # Type checks

    if not isinstance(waferName, str):
        raise TypeError('"waferName" must be a string.')

    wc = w.waferCollation(connection, waferName)


    def acronymsFromDSdefinition(DSdefintion, locGroupDict):

        acronyms = []
        for entry in DSdefintion:
            resName = entry['resultName']
            locations = locGroupDict[entry['locationGroup']]
            reqTags = entry['tagFilters']['required']

            for loc in locations:
                acronym = '_'.join([resName, loc]+reqTags)
                acronyms.append(acronym)
        
        return acronyms


    # 1. Acronyms
    # First I retrieve the datasheet definitions and I use it to generate a
    # dictionary that associates each chip label to its list of acronyms

    bps = wc.chipBlueprints
    if bps is None:
        raise Exception(f'No chip blueprints associated to wafer "{wc.wafer.name}".')

    bpIDs = [bp.ID for bp in bps]
    DSdefs = [bp.getDatasheetDefinition() for bp in bps]
    GrDicts = [bp.Locations.retrieveGroupsDict() for bp in bps]
    acronymLists = {ID: acronymsFromDSdefinition(DSD, GD) for ID, DSD, GD in zip(bpIDs, DSdefs, GrDicts)}
    
    # {<chipLabel>: [<list of acronyms>], ... }
    acronymDicts = {label: acronymLists[bp.ID] for label, bp in wc.chipBPdict.items()}

    # 2. Retrieve datasheet data from wafer collation
    DSdata = wc.Datasheets.retrieveData(returnDataFrame=True)

    if DSdata is None:
        raise FieldNotFound(f'Could not retrieve datasheet data for wafer "{wc.wafer.name}".')
    

    # 3. For each chip I retrieve the data for a given dashboard row
    
    allChipNames = DSdata['componentName'].unique()

    dashboardData = {}

    for chipName in allChipNames:
        chipData = DSdata[DSdata['componentName']==chipName]

        # print(f'Length of chipData: {len(chipData)}')
        if len(chipData) == 0:
            # print('Skipped')
            continue

        
        # print(f'Chip data keys: {chipData.keys()}')

        # 1. Generating all the dataframe keys
        dfKeys = ['wafer', 'chip', 'chipID', 'status', 'processStage']

        # print(f'chipLabel: {chipData["label"]}')
        chipLabel = chipData['label'].iloc[0]
        acronyms = acronymDicts[chipLabel] # List of all the acronyms

        dfKeys += acronyms

        rowDict = {key: None for key in dfKeys}

        # 2. Retrieving "global" data
       
        waferName = chipData['wafer'].iloc[0]
        chipID = chipData['componentID'].iloc[0]
        label = chipData['label'].iloc[0]
        status = wc.chipsDict[label].getField('status', verbose = False)
        processStage = wc.chipsDict[label].getField('processStage', verbose = False)
        
        rowDict['wafer'] = waferName
        rowDict['chip'] = chipName
        rowDict['chipID'] = chipID
        rowDict['status'] = status
        rowDict['processStage'] = processStage
            
        # 3. I retrieve the data and I populate the data fields
        chipData = chipData.reset_index()  # make sure indexes pair with number of rows
        for _, r in chipData.iterrows():
            resName = r['resultName']
            loc = r['location']
            reqTags = r['requiredTags']

            resValue = r['resultValue']

            if allResultDigits is False: # Digits based on error
                resError = r.get('resultError')
                resRepr = dataClass.valueErrorRepr(resValue, resError, valueDecimalsWithNoneError=2, printErrorPart=False)
                resValue = float(resRepr)
            
            acronym = '_'.join([resName, loc]+reqTags)
            rowDict[acronym] = resValue

        for key in dfKeys:
            if key not in dashboardData:
                dashboardData[key] = []

        for key, val in rowDict.items():
            dashboardData[key].append(val)

    dashboardDF = DataFrame(dashboardData)
    return dashboardDF

