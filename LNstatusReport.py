import mongomanager as mom

def _statusString(component):

    if not component.hasStatusLog():
        return '<No status>'
    
    entry = component.getField(['statusLog', -1], verbose = False)

    if entry is None:
        return '<No status>'

    status = entry['status']
    date = entry['dateOfChange']
    dateStr = date.strftime('(%Z) %Y/%m/%d - %H:%M:%S')

    return f'{dateStr} :: Status: "{status}"'

def _nameString(component, printIDs = False):

    if printIDs:
        if component.ID is None:
            IDstr = f'(ID: <{"-- No ID --":^22}>) '
        else:
            IDstr = f'(ID: {component.ID}) '
        
        nameStr = IDstr
    else:
        nameStr = ''

    name = component.name
    if name is None: name = "<No name>"

    symbols = component.symbolsString()
    if symbols is not None:
        name += f' {symbols}'

    nameStr += f'{name:20}'
    
    return nameStr


def printWaferStatus(connection:mom.connection,
    waferNames:list,
    *,
    printIDs = False,
    printWafers = True,
    printBars = True,
    printChips = False,
    excludeNoneStatus = False):

    def _retrieveWafer(waferName):
        
        query = {'name': {'$regex': waferName},
                'type': 'wafer'}

        wafer = mom.queryOne(connection, query, None,
            'beLaboratory', 'components', 'wafer')
        return wafer

    def _retrieveBars(waferName, wafer):
        
        query = {'name': {'$regex': waferName},
                'componentType': 'wafer bar'}

        bars = mom.query(connection, query, None,
            'beLaboratory', 'components', 'component')
        return bars

    def _retrieveChips(waferName, wafer):
        
        query = {'name': {'$regex': waferName},
                'componentType': {'$regex': 'chip'},
                'parentComponentID': wafer.ID}

        chips = mom.query(connection, query, None,
            'beLaboratory', 'components', 'component')
        return chips


    def _retrieveDocuments(waferName):

        mom.log.spare(f'Retrieving data related to "{waferName}".')

        wafer = _retrieveWafer(waferName)

        if wafer is not None:
            bars = _retrieveBars(waferName, wafer) if printBars else None
            chips = _retrieveChips(waferName, wafer) if printChips else None
        else:
            mom.log.error(f'Wafer "{waferName}" not found!')
            bars = None
            chips = None

        return wafer, bars, chips


    def _printWaferInfo(wafer):

        if not printWafers:
            return

        nameStr = f'{"[wafer]":>7} {_nameString(wafer, printIDs)}'
        statusStr = _statusString(wafer)
        string = f'{nameStr} :: {statusStr}'
        
        print(string)




    def _printBarInfo(bar):
        if not printBars:
            return

        nameStr = f'{"[bar]":>7} {_nameString(bar, printIDs)}'
        statusStr = _statusString(bar)
        string = f'{nameStr} :: {statusStr}'

        print(string)

    def _printChipInfo(chip):
        if not printChips:
            return

        nameStr = f'{"[chip]":>7} {_nameString(chip, printIDs)}'
        statusStr = _statusString(chip)
        string = f'{nameStr} :: {statusStr}'

        print(string)


    mom.log.info('Retrieving informations')

    resultsDict = {}

    for wafName in waferNames:
        mom.log.spare(f'Wafer name: {wafName}')
        wafer, bars, chips = _retrieveDocuments(wafName)

        msg = 'Found '
        if wafer is None:
            msg += 'none.'
        else:
            msg += 'wafer'
            if bars is not None:
                msg += ', bars'
            if chips is not None:
                msg += ', chips'
            msg += '.'

        mom.log.spare(msg)

        resultsDict[wafName] = {'wafer': wafer, 'bars': bars, 'chips': chips}

    
    print('\n\n-- STATUS REPORT --\n\n')
    
    for wafName in waferNames:

        wafer = resultsDict[wafName]['wafer']
        bars = resultsDict[wafName]['bars']
        chips = resultsDict[wafName]['chips']

        if bars is None:
            bars = []
        if chips is None:
            chips = []

        if resultsDict[wafName]['wafer'] is None:
            print(f'Wafer "{wafName}" not found.\n')
        else:
            
            if not printWafers:
                print(f"{wafName}")
            else:
                _printWaferInfo(wafer)

            for bar in bars:
                if bar.status is None:
                    if excludeNoneStatus:
                        continue
                _printBarInfo(bar)

            for chip in chips:
                if chip.status is None:
                    if excludeNoneStatus:
                        continue
                _printChipInfo(chip)
            print()
    
if __name__ == '__main__':

    print("""\n\n --- This is a demo script. ---
You should copy-paste it outside the library before modifying it,
otherwise it will be overwritten at the next Git pull.\n""")

    wafers = [
        # '2DR0003',
        # '2DR0006',
        '2DR0014',
        '2DR0015',
    ]

    mom.log.errorMode()
    conn = mom.connection('R&D', 'rdlab')
    printWaferStatus(conn, wafers, printChips = True,
    excludeNoneStatus = True)
    # In printWaferStatus() you can use
    #   > printWafers = True/False
    #   > printBars = True/False
    #   > printChips = True/False

