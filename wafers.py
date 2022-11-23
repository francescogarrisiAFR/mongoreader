import mongomanager as mom
import mongoreader.core as c

from mongoutils import isID
from mongoutils.connections import opened
from mongomanager import log





class waferCollation(c.collation):

    def __init__(self, connection:mom.connection, waferName_orCmp_orID,
            database:str = 'beLaboratory', collection:str = 'components'):

        if not isinstance(connection, mom.connection):
            raise TypeError('"connection" must be a mongomanager.connection object.')

        self.connection = connection

        if isinstance(waferName_orCmp_orID, mom.wafer):
            self.wafer = waferName_orCmp_orID
        else:
            self.wafer = self.collectWafer(waferName_orCmp_orID, database, collection)
        self.bars = self.collectBars()
        self.chips = self.collectChips()
        

    def collectWafer(self, waferName_orID:str,
        database:str = 'beLaboratory', collection:str = 'components'):

        if isinstance(waferName_orID, str):

            wafer = mom.queryOne(self.connection, {'name': {'$regex': waferName_orID}}, None,
            database, collection, 'native')
        
            if wafer is None:
                raise Exception(f'Could not find a wafer from string "{waferName_orID}".')

        else:
            if not isID(waferName_orID):
                raise TypeError('"waferName_orID" must be a string or an ID.')

            wafer = mom.importWafer(waferName_orID, self.connection)
            if wafer is None:
                raise Exception(f'Could not import a wafer from ID "{waferName_orID}".')

        log.info(f'Collected wafer "{wafer.name}"')
        return wafer

    
    def collectBars(self,
        database:str = 'beLaboratory', collection:str = 'components'):
        """Queries the database for bar object whose parent component is the wafer."""

        wafID = self.wafer.ID

        with opened(self.connection):
            bars = mom.query(self.connection,
                {'parentComponentID': wafID, 'componentType': 'wafer bar'},
                None,
                database, collection, 'native')

        log.info(f'Collected {len(bars)} bars')
        for ind, bar in enumerate(bars):
            log.spare(f'   {ind}: {bar.name}')
        return bars


    def collectChips(self):
        """Looks into the wafer children components to find the chips associated to the wafer."""

        chips = self.wafer.retrieveChildrenComponents(self.connection)

        log.info(f'Collected {len(chips)} chips')
        for ind, chip in enumerate(chips):
            log.spare(f'   {ind}: {chip.name}')

        return chips
        

    def refresh(self):
        """Refreshes all the components from the database."""
        raise NotImplementedError()

    @staticmethod
    def printWaferInfo(wafer, printIDs:bool = False):

        nameStr = f'{"[wafer]":>7} {_nameString(wafer, printIDs)}'
        statusStr = _statusString(wafer)
        string = f'{nameStr} :: {statusStr}'
        
        print(string)

    @staticmethod
    def printBarInfo(bar, printIDs:bool = False):

        nameStr = f'{"[bar]":>7} {_nameString(bar, printIDs)}'
        statusStr = _statusString(bar)
        string = f'{nameStr} :: {statusStr}'

        print(string)

    @staticmethod
    def printChipInfo(chip, printIDs:bool = False):

        nameStr = f'{"[chip]":>7} {_nameString(chip, printIDs)}'
        statusStr = _statusString(chip)
        string = f'{nameStr} :: {statusStr}'

        print(string)


    def printDashboard(self, *,
        printIDs = False,
        printWafer = True,
        printBars = True,
        printChips = True,
        excludeNoneStatus = False):
        """Prints a schematic view of the status of wafers, bars, chips.
        
        Keyword arguments:
            - printIDs = False, (bool)
            - printWafer = True, (bool)
            - printBars = True, (bool)
            - printChips = True, (bool)
            - excludeNoneStatus = False (bool)
        """

        if printWafer:
            self.printWaferInfo(self.wafer, printIDs)

        bars = [] if self.bars is None else self.bars
        chips = [] if self.chips is None else self.chips

        if printBars:
            for bar in bars:
                if bar.status is None:
                    if excludeNoneStatus:
                        continue
                self.printBarInfo(bar)

        if printChips:
            for chip in chips:
                if chip.status is None:
                    if excludeNoneStatus:
                        continue
                self.printChipInfo(chip)
        


class waferCollation_CDM(waferCollation):
    pass

class waferCollation_PAM4(waferCollation):

    def __init__(self, connection:mom.connection, waferName_orCmp_orID,
            database:str = 'beLaboratory', collection:str = 'components'):
        
        super().__init__(connection, waferName_orCmp_orID, database, collection)

        self.barsDict = self.defineBarsDict()
        self.chipsDict = self.defineChipsDict()


    def collectWafer(self, waferName:str,
        database:str = 'beLaboratory', collection:str = 'components'):

        log.debug('waferCollation_2DR8.collectWafer()')

        wafer = super().collectWafer(waferName, database, collection)

        if '2DR' not in wafer.name:
            log.warning(f'The collected wafer ("{wafer.name}") may not be a "2DR" wafer.')
    
        return wafer

    def defineBarsDict(self):

        def _keyCriterion(bar):
            return bar.name.split('-')[1]

        if self.bars is None:
            self.bars = self.collectBars()
        
        if self.bars is None:
            return {}

        return {_keyCriterion(bar): bar for bar in self.bars}

    def defineChipsDict(self):

        def _keyCriterion(chip):
            return chip.name.split('_')[1]

        if self.chips is None:
            self.chips = self.collectChips()

        if self.chips is None:
            return {}

        return {_keyCriterion(chip): chip for chip in self.chips}


class waferCollation_DR8(waferCollation):
    pass



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

    symbols = component.indicatorsString()
    if symbols is not None:
        name += f' ({symbols})'

    nameStr += f'{name:30}'
    
    return nameStr