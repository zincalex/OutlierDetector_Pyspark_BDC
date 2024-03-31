from pyspark import SparkContext, SparkConf
import sys
import os
import time
import math


def str_to_point(point):
    """ 
    Function that takes a string representing a point and returns it as a couple of integers
    """
    return (float(point.split(',')[0]), float(point.split(',')[1]))



def ExactOutliers(listOfPoints, D, M, K):
    """
    Compute all N(N-1)/2 pairwise distances among the points

    Parameters:
    listOfPoints (list) : list with with points as (cordinate_x, cordinate_y)
    D (float)           : distance to consider around a point, outliers distance 
    M (int)             : minimum number of points inside a cell to not be an outliers
    K (int)             : maximum number of points to be printed    
    """
    start_time = time.time()

    outliers = []
    num_outliers = 0
    for i in range(len(listOfPoints)) : 
        B_S_p = 0
        for j in range(len(listOfPoints)) :
            dist = math.sqrt((listOfPoints[i][0] - listOfPoints[j][0])**2 + ((listOfPoints[i][1] - listOfPoints[j][1])**2))
            if dist <= D : B_S_p += 1

        if B_S_p <= M : 
            num_outliers += 1
            outliers.append((listOfPoints[i], B_S_p))

    outliers = sorted(outliers, key=lambda x : x[1])
    
    print(f"Number of outliers = {num_outliers}")
    for k in range(min(K, len(outliers))) :   
        print(f"Point: {outliers[k][0]}")
    print(f"Running time of ExactOutliers = {time.time() - start_time} ms")
    


def count_points_per_cell(partition, LAMBDA):  
    """
    Count for the given partition how many points are inside each cells for the given partition

    Parameters:
    partition (RDD) : points inside a partition
    LAMBDA (float)  : lenght of the square sides of the cells

    Returns: 
    A list with key-value pairs, where the key are the cell indexes and the value is 
    the local amount of points for a partition inside a cell 
    """
    part_dict = {}
    for point in partition:
        cell_cord = (int(math.floor(point[0] / LAMBDA)), int(math.floor(point[1] / LAMBDA)))
        if cell_cord not in part_dict.keys():
            part_dict[cell_cord] = 1
        else: part_dict[cell_cord] += 1

    return [(key, part_dict[key]) for key in part_dict.keys()]



def count_neighborhood(cell, non_empty_cells_dict):
    """
    Add to the given cell the number of points inside the 3x3 grid and 7x7 grid around the cell 

    Parameters:
    cell (tuple)                      : (cell indexes(tuple), cell size (int))
    non_empty_cells_dict (dictionary) : dictionary with all the non empty cells and their respective cell size

    Returns: 
    A list with a single element [(cell indexes, (cell size, N3, N7))
    """
    size_3 = 3
    size_7 = 7
    
    N3 = 0
    N7 = 0
    for i in range(-(size_7 // 2), size_7 // 2 + 1):
        for j in range(-(size_7 // 2), size_7 // 2 + 1):
           key_shift = (cell[0][0] + i, cell[0][1] + j)
           N7 += non_empty_cells_dict.get(key_shift, 0) # If the key is found return the value, otherwise 0

           if ((i >= -1 and i <= 1) and (j >= -1 and j <= 1)) : # Inside the 3x3 area 
               N3 += non_empty_cells_dict.get(key_shift, 0)
 
    return  [(cell[0], (cell[1], N3, N7))]



def invert_key_value(RDD) :
    """
    Given an RDD, it swaps the key and the value 
    """
    return [(RDD[1], RDD[0])]



def MRApproxOutliers(inputPoints, D, M, K):
    """ 
    Consists of two main steps. Step A transforms the input RDD into an RDD whose elements corresponds to the non-empty cells and, 
    contain, for each cell, its identifier (i,j) and the number of points of S that it contains. The computation is be done by exploiting 
    the Spark partitions, without gathering together all points of a cell (which could be too many). 
    Step B transforms the RDD of cells, resulting from Step A, by attaching to each element, relative to a non-empty cell C, the values |N3(C)|
    and |N7(C)|, as additional info. It is assumed that the total number of non-empty cells is small with respect to the 
    capacity of each executor's memory, and therefore are downloaded in a local data structure.

    Parameters:
    inputPoints (RDD) : all points in the dataset
    D (float)         : distance to consider around a point, outliers distance 
    M (int)           : minimum number of points inside a cell to not be an outliers
    K (int)           : maximum number of points to be printed 
    """
    start_time = time.time()
    LAMBDA = D / (2*math.sqrt(2))

    # Step A 
    non_empty_cells = (inputPoints.mapPartitions(lambda partition: count_points_per_cell(partition, LAMBDA)) 
                                  .reduceByKey(lambda x,y : x + y))
    non_empty_cells_dict = non_empty_cells.collectAsMap()   # Dictionary with (cell indexes, size)     

    # Step B    
    outliers_RDD = (non_empty_cells.flatMap(lambda cell: count_neighborhood(cell, non_empty_cells_dict)))
    outliers = outliers_RDD.collect()   # List of (cell indexes, (size, N3, N7))

    true_outliers = 0
    uncertain_outliers = 0
    for elem in outliers :          # elem = ((index, index), (cellSize, N3, N7))
        if (elem[1][2] <= M): true_outliers += elem[1][0]
        elif (elem[1][1] <= M): uncertain_outliers += elem[1][0]

    print(f"Number of sure outliers = {true_outliers}")
    print(f"Number of uncertain outliers = {uncertain_outliers}")
    
    ordered_cells = (non_empty_cells.flatMap(invert_key_value)
                                        .sortByKey()
                                        .take(K))
    
    for i in ordered_cells :        # i = (cellSize, cell indexes)
        print(f"Cell: {i[1]}  Size = {i[0]}") 
    print(f"Running time of MRApproxOutliers = {time.time() - start_time} ms")    
    
    return 



def main():
    # CHECKING NUMBER OF CMD LINE PARAMETERS      0           1       2   3   4   5
    assert len(sys.argv) == 6, "Usage: python G023HW1.py <file_name> <D> <M> <K> <L>"
    
    # SPARK SETUP
    conf = SparkConf().setAppName('G023HW1')
    sc = SparkContext(conf=conf)
    
    # INPUT READING
    # 1. Read parameters
    D = sys.argv[2]
    D = float(D)
    
    M = sys.argv[3]
    assert M.isdigit(), "M must be an integer"
    M = int(M)
    
    K = sys.argv[4]
    assert K.isdigit(), "K must be an integer"
    K = int(K)
    
    L = sys.argv[5]
    assert L.isdigit(), "L must be an integer"
    L = int(L)

    # 2. Read input file 
    data_path = sys.argv[1]
    assert os.path.isfile(data_path), "File not found"
    rawData = sc.textFile(data_path)                                    # Read input points into an RDD of strings
    inputPoints = rawData.map(str_to_point).repartition(L).cache()      # (.distinct())Transform the RDD of strings into an RDD of points using the function str_to_point
    numPoints = inputPoints.count()                                     # Total number of points in the RDD
    
    # PRINTING PARAMETERS
    print(f"Dataset = {data_path.split("\\")[-1]} D={D} M={M} K={K} L={L}")
    print("Number of points = ", numPoints)
    
    
    if numPoints <= 200000 : # EXACT ALGORITHM
        listOfPoints = inputPoints.collect()
        ExactOutliers(listOfPoints, D, M, K)
    
    # APPROXIMATE ALGORITHM
    MRApproxOutliers(inputPoints, D, M, K)

    
if __name__ == "__main__":
    main()