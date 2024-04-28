import numpy as np
import arcpy as ap
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

#parameters
path = r"./data"
size_tile = 3    #the size of sub flow direction matrix
row = 9     #the row grid cells number of the complete flow direction matrix
col = 9     #the col grid cells number of the complete flow direction matrix

def multi_process(PARAMS):   #identify outlets location
    with ProcessPoolExecutor(max_workers = 1) as pool:
        pool.map(createraster,PARAMS)
    return 0

def createraster(PARAMS):
    outlets = PARAMS[0]
    size = PARAMS[1]
    currenttileid = PARAMS[2]
    currenttileoutlets = outlets[currenttileid]
    numpy_result = np.zeros((size,size),dtype='uint32')
    path_basin = path + r"\basin" + r'\No' + str(currenttileid) + "(basin).txt"
    file_basin = open(path_basin,"r")
    i = 0
    for line in file_basin.readlines():
        content = line.strip('.\n').split('.')
        currentbasinid = int(currenttileoutlets[i])
        i = i + 1
        for unit in content:
            rowandcol = unit.split(',')
            row = int(rowandcol[0])
            col = int(rowandcol[1])
            numpy_result[row][col] = currentbasinid
    file_basin.close()
    path_raster = path + r"\basin" + r'\No' + str(currenttileid) + "(basin).tif"
    temp_cellsizex = ap.GetRasterProperties_management(path_raster, "CELLSIZEX")
    cellsizex = float(temp_cellsizex.getOutput(0))
    cellsizey = cellsizex
    inraster = ap.Raster(path_raster, True)
    lowerleft_point = ap.Point(inraster.extent.XMin, inraster.extent.YMin)
    result = ap.NumPyArrayToRaster(numpy_result, lowerleft_point, cellsizex, cellsizey, 0)
    path_result = path + r"\finalresult" + r'\No' + str(currenttileid) + "(finalbasin).tif"
    result.save(path_result)
if __name__ == '__main__':
    ap.env.overwriteOutput = True
    path = path + r'\finalresult\result.txt'
    outlets = []
    file = open(path,'r')
    i = 0
    for line in file.readlines():
        outlets.append([])
        outlets[i].extend(line.strip('\n').split(' '))
        i = i + 1
    file.close()
    row_number = int(row/size_tile)
    col_number = int(col/size_tile)
    tile_number = row_number*col_number
    PARAMS = []
    for i in range(0, tile_number):
        PARAMS.append([outlets, size_tile, i])
    multi_process(PARAMS)