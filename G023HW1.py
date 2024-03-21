from pyspark import SparkContext, SparkConf
import sys
import os

def str_to_point(point):
    # Function that takes a string representing a point and returns it as a couple of integers
    return float(point.split(',')[0]), float(point.split(',')[1])

def ExactOutliers(listOfPoints, D, M, K):
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
    # 1. Read input file 
    data_path = sys.argv[1]
    assert os.path.isfile(data_path), "File not found"
    rawData = sc.textFile(data_path)                                    # Read input points into an RDD of strings
    inputPoints = rawData.map(str_to_point).repartition(1).cache()      # Transform the RDD of strings into an RDD of points using the function str_to_point
    
    # 2. Read parameters
    D = sys.argv[2]
    assert isinstance(D, float), "D must be a float"
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
    
    # PRINTING PARAMETERS
    print("Dataset = ", data_path.split("\\")[-1])
    print("D = ", D)
    print("M = ", M)
    print("K = ", K)
    print("L = ", L)
    
    # EXACT ALGORITHM
    

    # APPROXIMATE ALGORITHM



if __name__ == "__main__":
    main()