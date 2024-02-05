import mongoreader.connectors.MMSconnector as out
import mongomanager as mom
import mongoreader.modules as morm
from pathlib import Path

moduleFolder = Path(r'C:\Users\francesco.garrisi\Desktop\Test Dot Out Modules')
logFolder = moduleFolder

conn = mom.connection('R&D', 'rdlab')

batches = morm.queryModuleBatches(conn)

for bat in batches:

    logFile = logFolder / (bat.replace('/', '-') + '.log')
    mom.log.setFileLogging(logFile)
    mom.log.setFileLevel('IMPORTANT')

    mb = morm.moduleBatch(conn, bat)
    dout = out.DotOutManager_Modules(conn, moduleFolder)

    for mod in mb.modules:
        if mod.hasTestHistory():

            mom.log.important(f'GENERATING DOT OUT FOR MODULE "{mod.name}".')
            dout.saveDotOutLine(mod, False)

