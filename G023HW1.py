from pyspark import SparkContext, SparkConf
import sys
import os
import time
import math

def str_to_point(point):
    # Function that takes a string representing a point and returns it as a couple of integers
    return float(point.split(',')[0]), float(point.split(',')[1])

def ExactOutliers(listOfPoints, D, M, K):
    start_time = time.time()

    outliers = []
    num_outliers = 0
    for p in listOfPoints : 
        for q in listOfPoints : 
            B_S_p = 0
            if p != q :
                dist = math.sqrt((p[0] - q[0])**2 + ((p[1] - q[1])**2))
                if dist <= D : 
                    B_S_p += 1
                
        
        if B_S_p <= M : 
            num_outliers += 1
            outliers.append((p, B_S_p))

    
    outliers = sorted(outliers, key=lambda x : x[1])
    print(outliers)

    print("----------------------------------------------")
    print("Numbers of (%.2f, %d)-outliers : %d" %(D, M, num_outliers))
    for k in range(0, K) :   
        print(outliers[k][0])
    print("----------------------------------------------")
    


    print("ExactOutliers() running time: --- %s seconds ---" % (time.time() - start_time))
    return


def MRApproxOutliers(inputPoints, D, M, K):
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
    inputPoints = rawData.map(str_to_point).repartition(L).cache()      # Transform the RDD of strings into an RDD of points using the function str_to_point
    numPoints = inputPoints.count()                                     # Total number of points in the RDD
    
    # PRINTING PARAMETERS
    print("Dataset = ", data_path.split("\\")[-1])
    print("D = ", D)
    print("M = ", M)
    print("K = ", K)
    print("L = ", L)
    print("Total number of points :", numPoints)
    
    listOfPoints = inputPoints.collect()
    if numPoints <= 200000 : # EXACT ALGORITHM
        ExactOutliers(listOfPoints, D, M, K)
        
    # APPROXIMATE ALGORITHM
    MRApproxOutliers(listOfPoints, D, M, K)

    
    

    



if __name__ == "__main__":
    main()