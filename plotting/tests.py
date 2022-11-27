#%%

import mongomanager as mom
import mongoreader.plotting.waferPlotting as wplt
import mongoreader.wafers as morw


from numpy import random


conn = mom.connection('R&D', 'Francesco')

# wafer = morw.waferCollation_PAM4(conn, '2DR0014')

w = wplt.waferPlotter_PAM4()


# %%

def goggleMeasure(chip, measureName:str):
    
    if not isistance(measureName, str):
        raise TypeError('"measureName" must be a string.')
    
    outputDictionary = dict()
    for testEntry in chip['testHistory']:
        for result in testEntry['results']:
            if measureName==result['resultName']:
                if result.get('datasheetReady'):
                    outputDictionary[result['location']] = result['resultData']
    return outputDictionary

#%%

dataDict = {}
for ind in range(11):
    index = ind + 1
    dataDict[f'FR8-{index:02}'] = 2*random.random()




# w.plotData_chipScale(fakedataDict, 'DR80014', dataType = 'float', NoneColor=None)
w.plotData_chipScale(dataDict, 'Random data on FR8', dataType = 'float', NoneColor=(.8,.8,.8),
                    chipGroups=None, colorbarLabel='Data [a.u.]', waferName = '2CDM0007')


#%%

dataDict = {}

for ser in range(11):
    dataDict[f'DR8-{ser+1:02}'] = {f'MZ{ind+1}': random.random() for ind in range(8)}
    dataDict[f'FR8-{ser+1:02}'] = {f'MZ{ind+1}': random.random() for ind in range(8)}


w.plotData_subchipScale(dataDict, 'Random data on FR8', dataType = 'float', NoneColor=(0.8,.8,.8),
                    chipGroups=None, colorbarLabel='Data [a.u.]', waferName = '2CDM0007',
                    colormapName = 'inferno')
