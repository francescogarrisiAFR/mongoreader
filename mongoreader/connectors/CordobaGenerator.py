import mongoreader.connectors.MMSconnector as out
import mongomanager as mom
import mongoreader.wafers as morw
from pathlib import Path

chipFolder = Path(r'C:\Users\francesco.garrisi\Desktop\Test Dot Out Chips')
logFolder = chipFolder

conn = mom.connection('R&D', 'rdlab')

wafNames = morw.queryWafers(conn, waferType = 'CA')

for waf in wafNames:

    logFile = logFolder / (waf + '.log')
    mom.log.setFileLogging(logFile)
    mom.log.setFileLevel('IMPORTANT')

    wc = morw.waferCollation(conn, waf)
    dout = out.DotOutManager_Chips(conn, chipFolder)

    for chip in wc.chips:
        if chip.hasTestHistory():

            mom.log.important(f'GENERATING DOT OUT FOR CHIP "{chip.name}".')
            dout.saveDotOutLine(chip)

