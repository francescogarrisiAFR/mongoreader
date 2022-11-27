import json
import csv
from pathlib import Path

csvpath = Path(__file__).parent / 'BilbaoCoordinates.csv'

# This list maps the raw of the csv file with the serial number on chip

sequences = {
    'DR8': [5,10,6,11,2,1,4,3,8,9,7],
    'FR8': [7,5,6,1,2,3,4,8,10,11,9],
    'DR4': [17,18,19,20,4,7,1,10,11,2,3,9,8,5,12,13,15,16,14,6,23,22,21,24],
    'FR4': [19,10,16,17,18,15,2,1,6,9,8,7,3,4,5,14,13,12,11,22,23,21,20]}



keys = ['DR4', 'DR8', 'FR4', 'FR8']
centerPtsDict = {key: [] for key in keys}

sizes = {
    'DR4': [10000, 4000],
    'FR4': [10000, 5000],
    'DR8': [10000, 7000],
    'FR8': [10000, 8000],
    }

with open(csvpath) as csvfile:
    reader = csv.reader(csvfile)

    for row in reader:
        if row[0] in keys:
            centerPtsDict[row[0]].append([int(row[1]), int(row[2])])
            

ptsDict = {}

for key, cptsList in centerPtsDict.items():
    
    w = sizes[key][0]
    h = sizes[key][1]
    
    for index, cpt in enumerate(cptsList):
        
        cx, cy = cpt[0], cpt[1]
        
        x0 = cx-w/2
        x1 = cx+w/2
        
        y0 = cy - h/2
        y1 = cy + h/2
        
        p0 = [x0/1000, y0/1000]
        p1 = [x1/1000, y1/1000]
        
        ser = sequences[key][index]
        serial = f'{ser:02}'
        
        newKey = f'{key}-{serial}'

        ptsDict[newKey] = {'p1': p0, 'p2': p1}
        
        
finalDict = {
    'bars': {},
    'chips': ptsDict
    }

outpath = csvpath.with_suffix('.json')

with open(outpath, 'w') as file:
    json.dump(finalDict, file)
    
print(outpath)