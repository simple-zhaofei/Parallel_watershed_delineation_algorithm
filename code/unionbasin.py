import numpy as np
import arcpy as ap
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import copy
#parameters
size_tile = 3  #the size of sub flow direction matrixes
path = r"./data"
row = 9     #the row grid cells number of the complete flow direction matrix
col = 9     #the col grid cells number of the complete flow direction matrix

class Outlet:
    def __init__(self, row, col, type, currenttileid,currentbasinid, downstreamtileid,downstreambasinid, upstreaminfo):
        self.row = row
        self.col = col
        self.type = type
        self.currenttileid = currenttileid
        self.currentbasinid = currentbasinid
        self.downstreamtileid = downstreamtileid
        self.downstreambasinid = downstreambasinid
        self.upstreaminfo = upstreaminfo

class Raster_location:
    def __init__(self,upandright,up,upandleft,left,bottomandleft,bottom,bottomandright,right):
        self.upandright = upandright
        self.up = up
        self.upandleft = upandleft
        self.left = left
        self.bottomandleft = bottomandleft
        self.bottom = bottom
        self.bottomandright = bottomandright
        self.right = right

def Loction(row,col):
    list_raster_location = []
    for i in range(0,row):
        for j in range(0,col):
            raster_location = Raster_location(None, None, None, None, None, None, None, None)
            if i - 1 >= 0 and j - 1 >= 0:
                raster_location.upandleft = (i - 1) * col + j - 1
            if i - 1 >= 0:
                raster_location.up = (i - 1) * col + j
            if i - 1 >= 0 and j + 1 < col:
                raster_location.upandright = (i - 1) * col + j + 1
            if j - 1 >= 0:
                raster_location.left = i * col + j - 1
            if j + 1 < col:
                raster_location.right = i * col + j + 1
            if i + 1 < row and j - 1 >= 0:
                raster_location.bottomandleft = (i + 1) * col + j - 1
            if i + 1 < row:
                raster_location.bottom = (i + 1) * col + j
            if i + 1 < row and j + 1 < col:
                raster_location.bottomandright = (i + 1) * col + j + 1
            list_raster_location.append(raster_location)
    return list_raster_location

#标注所有的叶子出水口
def outlettype(numoftiles):
    outlets = []
    finaloutlets = []
    for i in range(0,numoftiles):
        outlets.append([])
        path_outlets = path + r'\outlets' +'/'+ 'No' + str(i) + '(outlets).txt'
        file_outlet = open(path_outlets)
        j = 0
        for line in file_outlet.readlines():
            j = j + 1
            txt_outlet = line.strip('\n').split(',')
            row_outlet = int(txt_outlet[0])
            col_outlet = int(txt_outlet[1])
            dir_outlet = int(txt_outlet[2])
            if dir_outlet == 0 or dir_outlet == 255:
                outlet = Outlet(row_outlet,col_outlet,1,i,j,-1,-1,upstreaminfo=[])
                finaloutlets.append(copy.deepcopy(outlet))
            else:
                outlet = Outlet(row_outlet,col_outlet,0,i,j,-1,-1,upstreaminfo=[])
            outlets[i].append(outlet)
    return outlets,finaloutlets

def union(PARAMS):
    currenttile = PARAMS[0]
    list_raster_location = PARAMS[1] 
    rowtile = PARAMS[2]
    coltile = PARAMS[3]             
    numpy_union = []
    path_currenttile = path+'/outlets/No'+str(currenttile)+'(outlets).txt'  
    upandleft = list_raster_location[currenttile].upandleft
    up = list_raster_location[currenttile].up
    upandright = list_raster_location[currenttile].upandright
    right = list_raster_location[currenttile].right
    bottomandright = list_raster_location[currenttile].bottomandright
    bottom = list_raster_location[currenttile].bottom
    bottomandleft = list_raster_location[currenttile].bottomandleft
    left = list_raster_location[currenttile].left                           
    if upandleft != None:
        path_upandleft = path + '/basin/'+str(upandleft) +'bottom.txt'    
        file_upandleft = open(path_upandleft,'r')
        numpy_upandleft = np.zeros((size_tile),'int16')
        i = 0
        for line in file_upandleft.readlines():
            numpy_upandleft[i] = int(line.strip('\n'))
            i = i + 1
        file_upandleft.close()
    else:
        numpy_upandleft = []

    if up != None:
        path_up = path + '/basin/'+str(up) +'bottom.txt'
        file_up = open(path_up,'r')
        numpy_up = np.zeros((size_tile),'int16')
        i = 0
        for line in file_up.readlines():
            numpy_up[i] = int(line.strip('\n'))
            i = i + 1
        file_up.close()
    else:
        numpy_up = []

    if upandright != None:
        path_upandright = path + '/basin/'+str(upandright) +'bottom.txt'
        file_upandright = open(path_upandright,'r')
        numpy_upandright = np.zeros((size_tile),'int16')
        i = 0
        for line in file_upandright.readlines():
            numpy_upandright[i] = int(line.strip('\n'))
            i = i + 1
        file_upandright.close()
    else:
        numpy_upandright = []

    if right != None:
        path_right = path + '/basin/' + str(right) + 'left.txt'
        file_right = open(path_right,'r')
        numpy_right = np.zeros((size_tile),'int16')
        i = 0
        for line in file_right.readlines():
            numpy_right[i] = int(line.strip('\n'))
            i = i + 1
        file_right.close()
    else:
        numpy_right = []

    if bottomandright != None:
        path_bottomandright = path + '/basin/'+str(bottomandright) +'up.txt'
        file_bottomandright = open(path_bottomandright,'r')
        numpy_bottomandright = np.zeros((size_tile),'int16')
        i = 0
        for line in file_bottomandright.readlines():
            numpy_bottomandright[i] = int(line.strip('\n'))
            i = i + 1
        file_bottomandright.close()
    else:
        numpy_bottomandright = []

    if bottom != None:
        path_bottom = path + '/basin/'+str(bottom) +'up.txt'
        file_bottom = open(path_bottom,'r')
        numpy_bottom = np.zeros((size_tile),'int16')
        i = 0
        for line in file_bottom.readlines():
            numpy_bottom[i] = int(line.strip('\n'))
            i = i + 1
        file_bottom.close()
    else:
        numpy_bottom = []

    if bottomandleft != None:
        path_bottomandleft = path + '/basin/'+str(bottomandleft) +'up.txt'
        file_bottomandleft = open(path_bottomandleft,'r')
        numpy_bottomandleft = np.zeros((size_tile),'int16')
        i = 0
        for line in file_bottomandleft.readlines():
            numpy_bottomandleft[i] = int(line.strip('\n'))
            i = i + 1
        file_bottomandleft.close()
    else:
        numpy_bottomandleft = []

    if left != None:
        path_left = path + '/basin/'+str(left) +'right.txt'
        file_left = open(path_left,'r')
        numpy_left = np.zeros((size_tile),'int16')
        i = 0
        for line in file_left.readlines():
            numpy_left[i] = int(line.strip('\n'))
            i = i + 1
        file_left.close()
    else:
        numpy_left = []


    file = open(path_currenttile,'r')
    i = 1
    for line in file.readlines():   
        row_outlet = int(line.strip('\n').split(',')[0])
        col_outlet = int(line.strip('\n').split(',')[1])
        dir_outlet = int(line.strip('\n').split(',')[2])
        if dir_outlet == 32:
            if row_outlet == 0 and col_outlet == 0 and numpy_upandleft != []:
               numpy_union.append([currenttile,i,upandleft,numpy_upandleft[coltile-1]])
            elif row_outlet == 0 and numpy_up != []:
                if col_outlet == 0:
                    continue
                numpy_union.append([currenttile,i,up,numpy_up[col_outlet-1]])
            elif col_outlet == 0 and numpy_left != []:
                if row_outlet == 0:
                    continue
                numpy_union.append([currenttile,i,left,numpy_left[row_outlet-1]])
        elif dir_outlet == 64 and numpy_up != []:
            numpy_union.append([currenttile,i,up,numpy_up[col_outlet]])
        elif dir_outlet == 128:
            if row_outlet == 0 and col_outlet == coltile - 1 and numpy_upandright != []:
                numpy_union.append([currenttile,i,upandright,numpy_upandright[0]])
            elif row_outlet == 0 and numpy_up != []:
                if col_outlet == coltile - 1:
                    continue
                numpy_union.append([currenttile,i,up,numpy_up[col_outlet+1]])
            elif col_outlet == coltile - 1 and numpy_right != []:
                if row_outlet == 0:
                    continue
                numpy_union.append([currenttile,i,right,numpy_right[row_outlet - 1]])
        elif dir_outlet == 1 and numpy_right != []:
            numpy_union.append([currenttile,i,right,numpy_right[row_outlet]])
        elif dir_outlet == 2:
            if row_outlet == rowtile - 1 and col_outlet == coltile - 1 and numpy_bottomandright != []:
                numpy_union.append([currenttile,i,bottomandright,numpy_bottomandright[0]])
            elif row_outlet == rowtile - 1 and numpy_bottom != []:
                if col_outlet == coltile - 1:
                    continue
                numpy_union.append([currenttile,i,bottom,numpy_bottom[col_outlet+1]])
            elif col_outlet == coltile - 1 and numpy_right != []:
                if row_outlet == rowtile - 1:
                    continue
                numpy_union.append([currenttile,i,right,numpy_right[row_outlet+1]])
        elif dir_outlet == 4 and numpy_bottom != []:
            numpy_union.append([currenttile,i,bottom,numpy_bottom[col_outlet]])
        elif dir_outlet == 8:
            if row_outlet == rowtile - 1 and col_outlet == 0 and numpy_bottomandleft != []:
                numpy_union.append([currenttile,i,bottomandleft,numpy_bottomandleft[coltile-1]])
            elif row_outlet == rowtile - 1 and numpy_bottom != []:
                if col_outlet == 0:
                    continue
                numpy_union.append([currenttile,i,bottom,numpy_bottom[col_outlet-1]])
            elif col_outlet == 0 and numpy_left != []:
                if row_outlet == rowtile - 1:
                    continue
                numpy_union.append([currenttile,i,left,numpy_left[row_outlet+1]])
        elif dir_outlet == 16 and numpy_left != []:
            numpy_union.append([currenttile,i,left,numpy_left[row_outlet]])
        i = i + 1
    file.close()
    return numpy_union

def multi_process(PARAMS):   #identify outlets location
    with ProcessPoolExecutor(max_workers = 1) as pool:
        result= pool.map(union,PARAMS)
    return result

if __name__ == '__main__':
    rowoftiles = int(row/size_tile)
    coloftiles = int(col/size_tile)
    totaloftiles = rowoftiles * coloftiles
    outlets,finaloutlets = outlettype(totaloftiles)
    list_raster_location = Loction(rowoftiles,coloftiles)
    PARAMS = []
    for i in range(0,totaloftiles):
        PARAMS.append([i,list_raster_location,size_tile,size_tile])
    numpy_unions = multi_process(PARAMS)
    for numpy_union in numpy_unions:
        for unit in numpy_union:
            currenttileid = unit[0]
            currentbasinid = unit[1] - 1
            downstreamtileid = unit[2]
            downstreambasinid = unit[3] - 1
            outlets[currenttileid][currentbasinid].downstreamtileid = downstreamtileid
            outlets[currenttileid][currentbasinid].downstreambasinid = downstreambasinid + 1
            outlets[downstreamtileid][downstreambasinid].upstreaminfo.append([currenttileid,currentbasinid + 1])
    basinid = 0
    for outlet in finaloutlets:
        basinid = basinid + 1
        currenttileid = outlet.currenttileid
        currentbasinid = outlet.currentbasinid - 1
        outlets[currenttileid][currentbasinid].currentbasinid = basinid
        numpy_processingbasins = []
        numpy_processingbasins.extend(outlets[currenttileid][currentbasinid].upstreaminfo)
        while (len(numpy_processingbasins) != 0):
            processingbasin = numpy_processingbasins.pop()
            currenttileid = processingbasin[0]
            currentbasinid = processingbasin[1] - 1
            numpy_processingbasins.extend(outlets[currenttileid][currentbasinid].upstreaminfo)
            outlets[currenttileid][currentbasinid].currentbasinid = basinid
    path_result = path + r'\finalresult\result.txt'
    file_basin = open(path_result, 'w')
    i = 0
    for tileoutlets in outlets:
        if i == 1:
            file_basin.write('\n')
        i = 1
        for outlet in  tileoutlets:
            file_basin.write(str(outlet.currentbasinid) + ' ')
