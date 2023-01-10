# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 23:30:23 2022

@author: Francesco.Garrisi
"""

import mongomanager as mom
# mom.log.debugMode()
import mongoreader.plotting.waferPlotting as wplt
import mongoreader.wafers as morw

conn = mom.connection('R&D', 'rdlab')

wc = morw.waferCollation_Budapest(conn, '2DR0014')
wc.plotChipStatus()

wp = wplt.waferPlotter(conn, 'Budapest')

def goggleMeasure(chip, measureName:str):
    
    if not isinstance(measureName, str):
        raise TypeError('"measureName" must be a string.')
    
    ctype = chip['componentType']
    if 'DR8' in ctype or 'FR8' in ctype:
        outputDictionary = {f'MZ{ind+1}': None for ind in range(8)}
    if 'DR4' in ctype or 'FR4' in ctype:
        outputDictionary = {f'MZ{ind+1}': None for ind in range(4)}

    for testEntry in chip['testHistory']:
        for result in testEntry['results']:
            if measureName==result['resultName']:
                if result.get('datasheetReady'):
                    
                    outputDictionary[result['location']] = result['resultData']['value']

    if all(v is None for v in outputDictionary.values()):
        return None
                    
    return outputDictionary

def dataDict(measureName:str, waferCollation):
    dataDict = {key: goggleMeasure(chip, measureName) for key, chip in waferCollation.chipsDict.items()}
    dataDict = {key: val for key, val in dataDict.items() if val is not None}
    return dataDict

measName = 'IL'
measUnit = 'dB'

wp.plotData_subchipScale(dataDict(measName, wc), dataType = 'float',
                        title = f'Test plot {measName}', waferName = wc.wafer.name,
                        colormapName = 'inferno', colorbarLabel = f'{measName} [{measUnit}]',
                        chipGroups = ['DR8'])