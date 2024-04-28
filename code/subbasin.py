import numpy as np
import arcpy as ap
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import queue
#parameters
size_tile = 3  #the size of sub flow direction matrixes
path = r"./data"
row = 9     #the row grid cells number of the complete flow direction matrix
col = 9     #the col grid cells number of the complete flow direction matrix

def delineate_watershed(PARAMS):
    path = PARAMS[0]
    flag = PARAMS[1]
    result = np.zeros((size_tile,size_tile),dtype='int16')-9999
    dir_path = path + '/' + 'No' + str(flag) + '(dir).tif'
    outlet_path = path + '/outlets/' + 'No' + str(flag) + '(outlets).txt'
    path_file_basin = path + '/basin/' + 'No' + str(flag) + '(basin).txt'
    path_up = path + '/basin/' + str(flag) + 'up.txt'
    path_right = path + '/basin/' + str(flag) + 'right.txt'
    path_bottom = path + '/basin/' + str(flag) + 'bottom.txt'
    path_left = path + '/basin/' + str(flag) + 'left.txt'
    file_basin = open(path_file_basin, 'w')

    numpy_dir = ap.RasterToNumPyArray(dir_path,nodata_to_value = 247)
    filename = outlet_path
    i = 0
    queue_seeds = queue.Queue()
    row_tile = size_tile
    col_tile = size_tile
    file_outlets = open(filename, 'r')
    for line in file_outlets.readlines():
        i += 1
        content = line.strip('\n').split(',')
        row = int(content[0])
        col = int(content[1])
        queue_seeds.put([row,col])
        while queue_seeds.qsize() > 0:
            [row,col] = queue_seeds.get()
            result[row][col] = i
            file_basin.write(str(row)+','+str(col)+'.')
            if row - 1 >= 0 and col - 1 >= 0:
                upstream_dir = numpy_dir[row-1][col-1]
                if upstream_dir == 2:
                    queue_seeds.put([row-1,col-1])
            if row - 1 >= 0:
                upstream_dir = numpy_dir[row-1][col]
                if upstream_dir == 4:
                    queue_seeds.put([row-1,col])
            if row - 1 >= 0 and col + 1 < col_tile:
                upstream_dir = numpy_dir[row-1][col+1]
                if upstream_dir == 8:
                    queue_seeds.put([row-1,col+1])
            if col - 1 >= 0:
                upstream_dir = numpy_dir[row][col-1]
                if upstream_dir == 1:
                    queue_seeds.put([row,col-1])
            if col + 1 < col_tile:
                upstream_dir = numpy_dir[row][col+1]
                if upstream_dir == 16:
                    queue_seeds.put([row,col+1])
            if row + 1 < row_tile and col - 1 >= 0:
                upstream_dir = numpy_dir[row+1][col-1]
                if upstream_dir == 128:
                    queue_seeds.put([row+1,col-1])
            if row + 1 < row_tile:
                upstream_dir = numpy_dir[row+1][col]
                if upstream_dir == 64:
                    queue_seeds.put([row+1,col])
            if row + 1 < row_tile and col + 1 < col_tile:
                upstream_dir = numpy_dir[row+1][col+1]
                if upstream_dir == 32:
                    queue_seeds.put([row+1,col+1])
        file_basin.write('\n')
    file_basin.close()
    file_outlets.close()

    watershed_path = path + '/basin/' + 'No' + str(flag) + '(basin).tif'
    temp_cellsizex = ap.GetRasterProperties_management(dir_path, "CELLSIZEX")
    cellsizex = float(temp_cellsizex.getOutput(0))
    cellsizey = cellsizex
    inraster = ap.Raster(dir_path, True)
    lowerleft_point = ap.Point(inraster.extent.XMin, inraster.extent.YMin)
    raster_result = ap.NumPyArrayToRaster(result, lowerleft_point, cellsizex, cellsizey, -9999)
    raster_result.save(watershed_path)

    file_up = open(path_up, 'w')
    file_right = open(path_right, 'w')
    file_bottom = open(path_bottom, 'w')
    file_left = open(path_left, 'w')
    for i in range(0,row_tile):
        content_up = str(result[0][i])+'\n'
        content_bottom = str(result[row_tile-1][i])+'\n'
        content_left = str(result[i][0])+'\n'
        content_right = str(result[i][col_tile-1])+'\n'
        file_up.write(content_up)
        file_right.write(content_right)
        file_bottom.write(content_bottom)
        file_left.write(content_left)
    file_up.close()
    file_right.close()
    file_bottom.close()
    file_left.close()

def multi_process(PARAMS):
    with ProcessPoolExecutor(max_workers = 5) as pool:
        pool.map(delineate_watershed,PARAMS)

if __name__ == '__main__':
    ap.env.workspace = path
    result_path = path + "/basin"
    PARAMS = []
    row_number = int(row / size_tile)
    col_number = int(col / size_tile)
    tile_number = row_number * col_number
    for i in range(0, tile_number):
        PARAMS.append([path,i])
    multi_process(PARAMS)
