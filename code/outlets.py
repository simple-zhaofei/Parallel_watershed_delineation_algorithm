import numpy as np
import arcpy as ap
from concurrent.futures import  ProcessPoolExecutor
#parameters
path_subflowdirection_tile = r".\data" #The place storing these flow direction matrix
path_outlet_txt = r".\data\outlets" #The place storing outlets text file
size_tile = 3    #the size of sub flow direction matrix
row = 9     #the row grid cells number of the complete flow direction matrix
col = 9     #the col grid cells number of the complete flow direction matrix

def Outlets(PARAMS):
    path = PARAMS[0]
    flag = PARAMS[1]
    direction = path + "\\" + "No" + str(flag) +"(dir).tif"
    numpy_dir = ap.RasterToNumPyArray(direction,nodata_to_value = -9999)
    row,col = np.shape(numpy_dir)
    outlets = []
    for i in range(0,row):
        for j in range(0,col):
            if numpy_dir[i][j] == 128:
                downstream_x = i - 1
                downstream_y = j + 1
                if downstream_x < 0 and downstream_y >= col:
                    outlets.append((i,j,128))
                elif downstream_x < 0:
                    outlets.append((i,j,128))
                elif downstream_y >= col:
                    outlets.append((i,j,128))
            elif numpy_dir[i][j] == 64:
                downstream_x = i - 1
                if downstream_x < 0:
                    outlets.append((i,j,64))
            elif numpy_dir[i][j] == 32:
                downstream_x = i - 1
                downstream_y = j - 1
                if downstream_x < 0 and downstream_y < 0:
                    outlets.append((i,j,32))
                elif downstream_x < 0:
                    outlets.append((i,j,32))
                elif downstream_y < 0:
                    outlets.append((i,j,32))
            elif numpy_dir[i][j] == 16:
                downstream_y = j - 1
                if downstream_y < 0:
                    outlets.append((i,j,16))
            elif numpy_dir[i][j] == 8:
                downstream_x = i + 1
                downstream_y = j - 1
                if downstream_x >= row and downstream_y < 0:
                    outlets.append((i,j,8))
                elif downstream_x >= row:
                    outlets.append((i,j,8))
                elif downstream_y < 0:
                    outlets.append((i,j,8))
            elif numpy_dir[i][j] == 4:
                downstream_x = i + 1
                if downstream_x >= row:
                    outlets.append((i,j,4))
            elif numpy_dir[i][j] == 2:
                downstream_x = i + 1
                downstream_y = j + 1
                if downstream_x >= row and downstream_y >= col:
                    outlets.append((i,j,2))
                elif downstream_x >= row:
                    outlets.append((i,j,2))
                elif downstream_y >= col:
                    outlets.append((i,j,2))
            elif numpy_dir[i][j] == 1:
                downstream_y = j + 1
                if downstream_y >= col:
                    outlets.append((i,j,1))
            elif numpy_dir[i][j] == 0:
                outlets.append((i,j,0))
            elif numpy_dir[i][j] == 255:
                outlets.append((i,j,255))
    return [outlets,flag]

def multi_process1(PARAMS):   #identify outlets location
    with ProcessPoolExecutor(max_workers = 5) as pool:
        result= pool.map(Outlets,PARAMS)
    return result

if __name__ == '__main__':
    PARAMS = []
    row_number = int(row/size_tile)
    col_number = int(col/size_tile)
    tile_number = row_number*col_number
    for i in range(0,tile_number):
        path = path_subflowdirection_tile
        PARAMS.append([path,i])
    result= multi_process1(PARAMS)
    for outlets in result:
        j = outlets[1]
        filename = path_outlet_txt + "/No" + str(j) + "(outlets).txt"
        file = open(filename, 'w')
        for outlet in outlets[0]:
            content = str(outlet[0]) + ',' + str(outlet[1]) + ',' + str(outlet[2]) + '\n'
            file.write(content)
        file.close()
