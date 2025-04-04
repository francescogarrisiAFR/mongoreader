import json
import csv
from pathlib import Path

csvpath = Path(__file__).parent / 'BilbaoCoordinates.csv'

with open(csvpath) as csvfile:
    reader = csv.reader(csvfile)

    serials = []
    xs = []
    ys = []

    for row in reader:
        ser, x, y = row

        ind = int(ser.rsplit('_', maxsplit = 1)[1])

        if 'DF' in ser:
            ser = 'DF'
        elif 'SE' in ser:
            ser = 'SE'
        elif 'DIM' in ser:
            ser = 'MZ'

        serial = f'{ind:02}-{ser}'

        serials.append(serial)
        xs.append(round(float(x)/1000, 3))
        ys.append(round(float(y)/1000, 3))

sizes = {
    'MZ': [14, 2.4],
    'SE': [14, 4.9],
    'DF': [14, 4.9],
    }

psMZ = []
psSE = []
psDF = []

ptsDict = {}
for fser, x, y in zip(serials, xs, ys):

    print(f'{fser}: {x} - {y}')

    if 'MZ' in fser:
        x0 = x - sizes['MZ'][0]/2
        x1 = x + sizes['MZ'][0]/2
        y0 = y - sizes['MZ'][1]/2
        y1 = y + sizes['MZ'][1]/2

    if 'SE' in fser:
        x0 = x - sizes['SE'][0]/2
        x1 = x + sizes['SE'][0]/2
        y0 = y - sizes['SE'][1]/2
        y1 = y + sizes['SE'][1]/2
        
    if 'DF' in fser:
        x0 = x - sizes['DF'][0]/2
        x1 = x + sizes['DF'][0]/2
        y0 = y - sizes['DF'][1]/2
        y1 = y + sizes['DF'][1]/2
    
    p1 = (round(x0, 3), round(y0, 3))
    p2 = (round(x1, 3), round(y1, 3))

    ptsDict[fser] = {'p1': p1, 'p2': p2}

print(psMZ)

labels = []

finalDict = {
    'bars': {},
    'chips': ptsDict
    }

print(ptsDict)

outpath = csvpath.with_suffix('.json')

with open(outpath, 'w') as file:
    json.dump(finalDict, file)
    
print(outpath)