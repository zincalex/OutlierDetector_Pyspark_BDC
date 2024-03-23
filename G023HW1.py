from pyspark import SparkContext, SparkConf
import sys
import os
import time
import math
import matplotlib.pyplot as plt

def plot_points(points):
    
    # Extract x and y coordinates from the list of points
    x_coords = [point[0] for point in points]
    y_coords = [point[1] for point in points]

    # Create a scatter plot
    plt.scatter(x_coords, y_coords, color='blue', label='Points')

    # Set labels and title
    plt.xlabel('X-axis')
    plt.ylabel('Y-axis')
    plt.title('Plot of Points')

    # Add legend
    plt.legend()

    # Display the plot
    plt.grid(True)
    plt.show()

def str_to_point(point):
    # Function that takes a string representing a point and returns it as a couple of integers
    return float(point.split(',')[0]), float(point.split(',')[1])

def ExactOutliers(listOfPoints, D, M, K):
    start_time = time.time()

    outliers = []
    num_outliers = 0
    for i in range(len(listOfPoints)) : 
        B_S_p = 0

        for j in range(len(listOfPoints)) :
            if(j != i) : 
                dist = math.sqrt((listOfPoints[i][0] - listOfPoints[j][0])**2 + ((listOfPoints[i][1] - listOfPoints[j][1])**2))
                if dist <= D : B_S_p += 1

        if B_S_p <= M : 
            num_outliers += 1
            outliers.append((listOfPoints[i], B_S_p))

    outliers = sorted(outliers, key=lambda x : x[1])
    print("----------------------------------------------")
    print("Numbers of (%.2f, %d)-outliers : %d" %(D, M, num_outliers))
    for k in range(min(K, len(outliers))) :   
        print(outliers[k][0])
    print("----------------------------------------------")
    print("ExactOutliers() running time: --- %s seconds ---" % (time.time() - start_time))

# Find the cell where a point is
def find_cell(point, cell_size):    # cell_size is LAMBDA
    x = point[0]
    y = point[1]
    i = int(x / cell_size)
    j = int(y / cell_size)
    return (i, j)

# Count the number of points in the same cell
def count_points_per_cell(inputPoints, D):
    cell_dict = {}
    for point in inputPoints:
        cell_cord = find_cell(point, D)
        if cell_cord not in cell_dict.keys():
            cell_dict[cell_cord] = 1
        else: cell_dict[cell_cord] += 1

    return [(key, cell_dict[key]) for key in cell_dict.keys()]

def gather_pairs_partitions(pairs):
    dic = {}
    for p in pairs :
        cell, loc_num_points = p[0], p[1]
        if cell not in dic.keys():
            dic[cell] = loc_num_points
        else: dic[cell] += loc_num_points
    return [(key, dic[key]) for key in dic.keys()]


# Count how many points are in the neighborhood size x s
def count_in_neighborhood(cell, size):
    cell_i = cell[0][0]
    cell_j = cell[0][1]
    neighbors = []
    for i in range(-(size // 2), size // 2 + 1):
        for j in range(-(size // 2), size // 2 + 1):
            neighbors.append((cell_i + i, cell_j + j), 1)
    return neighbors

def MRApproxOutliers(inputPoints, D, M, K):
    # Divide R^2 into matrix of size LAMBDA where (i*LAMBDA, j*LAMBDA) are the real coordinates
    LAMBDA = D/2*math.sqrt(2)

    # Step A 
    # TODO might consider to update flatMap -> map (map used for functions 1 on 1, that is our case since point -> unique cell)
    R1 = (inputPoints.flatMap(lambda points: count_points_per_cell(points, LAMBDA)) # MAP PHASE R1 (flatMap applies the function independently to each partition.)
                     .mapPartitions(gather_pairs_partitions) #reduce phase R1 (Return a new RDD by applying a function to each partition of this RDD)
                     .groupByKey() # SHUFFLE + GROUPING
                     .mapValues(lambda num_points : sum(num_points))) #REDUCE PHASE R2 (no map phase here)

    # Step B - 3x3 and 7x7 neighborhood
    neighborhood_3x3_sum = (R1.flatMap(lambda cell: count_in_neighborhood(cell[0], 3))  # cell[0] contains the cell coordinates
                          .map(lambda cell: cell[1])                                    # cell[1] contains the count of points from R1
                          .sum())
    neighborhood_7x7_sum = (R1.flatMap(lambda cell: count_in_neighborhood(cell[0], 7)) 
                          .map(lambda cell: cell[1])
                          .sum())

    return R1.sum()


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
    
    
    if numPoints <= 200000 : # EXACT ALGORITHM
        listOfPoints = inputPoints.collect()
        ExactOutliers(listOfPoints, D, M, K)
    

    #plot_points(listOfPoints)
    """
    # Print partitions
    partition_data = inputPoints.glom().collect()
    for i, partition in enumerate(partition_data):
        print(f"Partition {i}: {partition}")
    """
    
    # APPROXIMATE ALGORITHM
    #MRApproxOutliers(inputPoints, D, M, K)

    
    

    



if __name__ == "__main__":
    main()