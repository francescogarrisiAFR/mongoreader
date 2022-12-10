from mongomanager import log
import matplotlib.pyplot as plt
import matplotlib.patches as ptch
from numpy import pi, linspace, arccos, sin, cos

def waferPatch(D:float, notch:float = None, linewidth = 2):
    """The patch that represents the wafer edge.
    
    D is the diameter in mm.
    
    notch is the size in mm by which the notch is carved at the bottom."""

    if notch is None:
        return ptch.Circle((0, 0), D/2, color='black',fill=False,
            linewidth = linewidth)
    else:
        angle = arccos((D-2*notch)/D)
        # print(f'DEBUG: angle [deg] = {angle*180/pi}')
        pts = [[D*sin(a)/2, -D*cos(a)/2] for a in linspace(angle, 2*pi-angle, 100)]
        # print(f'DEBUG: pts = {pts}')

        return ptch.Polygon(pts,
            color = 'black', fill = False,
            closed = True, lw = linewidth)


def rectPatch(p1, p2, fillColor = None, linewidth = 1):

    x0, _, y0, _, width, height = _unpackP12(p1, p2)

    p = (x0, y0)

    if fillColor is None:
        color = None
        fill = False
    else:
        color = fillColor
        fill = True
        
    patch = ptch.Rectangle(p, width, height,
            edgecolor = 'black',
            facecolor=color,
            lw = linewidth,
            fill=fill)

    return patch


def rectSubPatches(p1, p2, subSections = 2, colors:list = None,
    direction:str = 'V', linewidth = 1,
    *,
    reversedDirection:bool = False):

    if not isinstance(subSections, int):
        raise TypeError('"subSections" must be a positive integer (not 0).')
    
    if subSections <= 0:
        raise ValueError('"subSections" must be a positive integer (not 0).')

    if colors is not None:
        if not isinstance(colors, list):
            raise TypeError('"colors" must be a list of colors or None.')
        else:
            if len(colors) != subSections:
                raise ValueError(f'The length of "colors" ({len(colors)}) must equal subSections ({subSections} in this case).')


    x1, x2, y1, y2, _, _ = _unpackP12(p1, p2)

    if direction == 'V':

        ystart = max(y1, y2)
        ystop = min(y1, y2)
        ys = list(linspace(ystart, ystop, subSections + 1))
        if reversedDirection: ys = reversed(ys)
        y1s = ys[:-1]
        y2s = ys[1:]
        p1s = [[x1, y] for y in y1s]
        p2s = [[x2, y] for y in y2s]
       

    elif direction == 'H':
        xstart = min(x1, x2)
        xstop = max(x1, x2)
        xs = list(linspace(xstart, xstop, subSections + 1))
        if reversedDirection: xs = reversed(xs)
        x1s = xs[:-1]
        x2s = xs[1:]
        p1s = [[x, y1] for x in x1s]
        p2s = [[x, y2] for x in x2s]

    if colors is None:
        colors = [None for _ in range(p1s)]
    
    rects = [rectPatch(p1, p2, c, linewidth = linewidth)
                for p1, p2, c in zip(p1s, p2s, colors)]

    return rects


def _unpackP12(p1, p2):
    """Returns x0, x1, y0, y1, width, height."""

    x0 = min(p1[0], p2[0])
    x1 = max(p1[0], p2[0])

    y0 = min(p1[1], p2[1])
    y1 = max(p1[1], p2[1])
    
    width = x1 - x0
    height = y1 - y0
    
    return x0, x1, y0, y1, width, height


