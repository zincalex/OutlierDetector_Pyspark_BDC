from pyspark import SparkContext, SparkConf, StorageLevel
import sys
import os
import time
import math
import random


# SPARK SETUP
conf = SparkConf().setAppName('G023HW2')
sc = SparkContext(conf=conf)


def str_to_point(point):
    """ 
    Function that takes a string representing a point and returns it as a couple of integers
    """
    return (float(point.split(',')[0]), float(point.split(',')[1]))



def euclidean_distance(p1, p2): 
    """ 
    Function that takes two points in the euclidean space and return their euclidean distance
    """
    return math.sqrt((p1[0] - p2[0])**2 + ((p1[1] - p2[1])**2))
    


def find_clustering_radius(point, Kcenters_brodcast):
    """
    This method calculate dist(x, S) = min d(x, y) with y ∈ S and d as euclidean distance.
 

    Parameters:
    point (tuple of float)          : points inside a partition
    kcenters_brodcast  (brodcast)   : brodcast variable that contains in its .value parameter a list of centers
    """
    return min(euclidean_distance(point, center) for center in Kcenters_brodcast.value)



def SequentialFFT(P, K):
    """ 
    This method implements the Farthest-First Traversal algorithm.
    
    Parameters:
    P (list) : set of points
    K (int)  : number of centers to consider
    """
    if(len(P) <= K) : 
        return []
    
    C = []
    C.append(P[0]) # Appending the first center c1 

    listOfPoints = {point: float('inf') for point in P}  
    listOfPoints.pop(C[0]) # Removing the last added center to avoid extra computations
    
    for i in range(K) : # O(K)
        farthest_dist = -1
        farthest_point_key = None
        newDist = 0.0
        for key in listOfPoints.keys() : # O(N) ---> For each point update the value (center at minimum distance)
            newDist = euclidean_distance(key, C[i])

            if newDist < listOfPoints[key] : # Update of the distance of the closest center
               listOfPoints[key] = newDist

            if listOfPoints[key] > farthest_dist :
                farthest_dist = listOfPoints[key]
                farthest_point_key = key

        listOfPoints.pop(farthest_point_key)
        C.append(farthest_point_key)  

    return C



def RDD_to_list(partition) : 
    """
    Transform an given partition into a list

    Parameters:
    partition (RDD) : points inside a partition
    K (int)         : number of centers

    partition_elements = list(partition)
    for point in partition_elements : 
    return [row for row in partition_elements]
    
    """
    return list(partition)



def MRFFT(inputPoints, K):
    """ 
    This method implements the MR-FarthestFirstTraversal algorithm.
    Rounds 1 and 2 compute a set C of K centers, using the MR-FarthestFirstTraversal algorithm described in class. The coreset computed in Round 1, 
    must be gathered in a list and in Round 2, the centers are obtained by running SequentialFFT on the coreset.
    Round 3 computes and returns the radius R of the clustering induced by the centers, that is the maximum, over all points x ∈ P, of the distance dist(x,C).
    
    Parameters:
    inputPoints (RDD) : all points in the dataset
    K (int)           : number of centers
    """

    # Round 1
    start_time = time.time() * 1000
    coreset_RDD = (inputPoints.mapPartitions(lambda partition: SequentialFFT(RDD_to_list(partition), K))).persist(StorageLevel.MEMORY_AND_DISK)
    coreset_RDD.count()  # Dummy action
    print(f"Running time of MRFFT Round 1 = {time.time()*1000 - start_time} ms") 

    # Round 2
    start_time = time.time() * 1000
    coreset = coreset_RDD.collect()
    kCenters = SequentialFFT(coreset, K)
    print(f"Running time of MRFFT Round 2 = {time.time()*1000 - start_time} ms") 

    # Round 3  
    start_time = time.time() * 1000
    kCenters_brodcast = sc.broadcast(kCenters)
    radius = inputPoints.map(lambda point : find_clustering_radius(point, kCenters_brodcast)).reduce(max)
    print(f"Running time of MRFFT Round 3 = {time.time()*1000 - start_time} ms") 

    return radius



def count_points_per_cell(partition, LAMBDA):  
    """
    Count for the given partition how many points are inside each cells for the given partition

    Parameters:
    partition (itertools.chain) : iterator to points inside a partition
    LAMBDA (float)              : lenght of the square sides of the cells

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
 
    return  (cell[0], (cell[1], N3, N7))



def invert_key_value(RDD) :
    """
    Given an RDD, it swaps the key and the value 
    """
    return (RDD[1], RDD[0])



def MRApproxOutliers(inputPoints, D, M):
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
    start_time = time.time() * 1000
    LAMBDA = D / (2*math.sqrt(2))

    # Step A 
    non_empty_cells = (inputPoints.mapPartitions(lambda partition: count_points_per_cell(partition, LAMBDA)) 
                                  .reduceByKey(lambda x,y : x + y))
    non_empty_cells_dict = non_empty_cells.collectAsMap()   # Dictionary with (cell indexes, size)     

    # Step B    
    outliers_RDD = (non_empty_cells.map(lambda cell: count_neighborhood(cell, non_empty_cells_dict)))
    outliers = outliers_RDD.collect()   # List of (cell indexes, (size, N3, N7))

    true_outliers = 0
    uncertain_outliers = 0
    for elem in outliers :          # elem = ((index, index), (cellSize, N3, N7))
        if (elem[1][2] <= M): true_outliers += elem[1][0]
        elif (elem[1][1] <= M): uncertain_outliers += elem[1][0]

    print(f"Number of sure outliers = {true_outliers}")
    print(f"Number of uncertain outliers = {uncertain_outliers}")
    print(f"Running time of MRApproxOutliers = {time.time()*1000 - start_time} ms")    
    
    return 



def main():
    # CHECKING NUMBER OF CMD LINE PARAMETERS  
    if len(sys.argv) != 5 :
        raise AssertionError("Usage: python G023HW1.py <file_name> <M> <K> <L>")
    
    # INPUT READING
    # 1. Read parameters    
    M = sys.argv[2]
    if not M.isdigit() :
        raise AssertionError("M must be an integer")
    M = int(M)
    
    K = sys.argv[3]
    if not K.isdigit() :
        raise AssertionError("K must be an integer")
    K = int(K)
    
    L = sys.argv[4]
    if not L.isdigit() :
        raise AssertionError("L must be an integer")
    L = int(L)

    # 2. Read input file 
    data_path = sys.argv[1]                                             # We removed the check for controlling if the file exists because CloudVeneto had multiple file systems and os.system couldn't handle this option
    rawData = sc.textFile(data_path)                                    # Read input points into an RDD of strings
    inputPoints = rawData.map(str_to_point).repartition(L).cache()      
    numPoints = inputPoints.count()                                    
    
    # PRINTING PARAMETERS
    print(f"{data_path} M={M} K={K} L={L}")
    print(f"Number of points = {numPoints}")

    # MR-FARTHESTFIRSTTRAVERSAL ALGORITHM
    D = MRFFT(inputPoints, K)
    print(f"Radius = {D}")

    # APPROXIMATE ALGORITHM
    MRApproxOutliers(inputPoints, D, M)
    
    sc.stop()

if __name__ == "__main__":
    main()