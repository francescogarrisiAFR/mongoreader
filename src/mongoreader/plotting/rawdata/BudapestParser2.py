import json
import csv
from pathlib import Path
from itertools import groupby

csvpath = Path(__file__).parent / 'BudapestCoordinates.csv'

# This list maps the raw of the csv file with the serial number on chip

keys = ['DR4', 'DR8', 'FR4', 'FR8']
centerPtsDict = {key: [] for key in keys}

with open(csvpath) as csvfile:
    reader = csv.reader(csvfile)

    for row in reader:
        if row[0] in keys:
            centerPtsDict[row[0]].append([int(row[1]), int(row[2])])


def sortPoints(pointsList):
    
    points = sorted(pointsList, key = lambda x: x[0]) # Sort by x
    
    
    grouped = groupby(points, key = lambda x: x[0])
    
    lists = []
    for key, group in grouped:
        lists.append(list(group))
    
    lists = [sorted(sublist, key = lambda x: -x[1]) for sublist in lists] # Sort by -y
    

    sortedPoints = []
    for sl in lists:
        sortedPoints += sl
        
    return sortedPoints

sortedDict = {key: sortPoints(value) for key, value in centerPtsDict.items()}

ptsDict = {}
for key in sortedDict:
    
    pts = sortedDict[key]
    newKeys = [f'{key}-{ind+1:02}' for ind in range(len(pts))]
    
    for nkey, pt in zip(newKeys, pts):
        ptsDict[nkey] = pt


outpath = csvpath.with_suffix('.json')
outpath = outpath.with_stem(csvpath.stem + '_centers')

with open(outpath, 'w') as file:
    json.dump(ptsDict, file)
    
print(outpath)