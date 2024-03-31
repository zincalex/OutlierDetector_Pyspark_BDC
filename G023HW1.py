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
    return (float(point.split(',')[0]), float(point.split(',')[1]))

def ExactOutliers(listOfPoints, D, M, K):
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
    
# Count how many points are in the neighborhood 
def count_neighborhood(cell_size, cell_size_dict):
    size_3 = 3
    size_7 = 7
    
    N3 = 0
    N7 = 0
    for i in range(-(size_7 // 2), size_7 // 2 + 1):
        for j in range(-(size_7 // 2), size_7 // 2 + 1):
           key = (cell_size[0][0] + i, cell_size[0][1] + j)
           N7 += cell_size_dict.get(key, 0) # If the key is found return the value, otherwise 0

           if ((i >= -1 or i <= 1) and (j >= -1 or j <= 1)) : # Inside the 3x3 area 
               N3 += cell_size_dict.get(key, 0)

       
    return  [(cell_size[0], (cell_size[1], N3, N7))]


def count_points_per_cell(partitions, LAMBDA):  
    part_dict = {}
    for point in partitions:
        cell_cord = (int(point[0] / LAMBDA), int(point[1] / LAMBDA))
        if cell_cord not in part_dict.keys():
            part_dict[cell_cord] = 1
        else: part_dict[cell_cord] += 1

    return [(key, part_dict[key]) for key in part_dict.keys()]

def invert_key_value(RDD) :
    return [RDD[1], RDD[0]]

def MRApproxOutliers(inputPoints, D, M, K):
    start_time = time.time()
    LAMBDA = D/2*math.sqrt(2)

    
    # Step A 
    non_empty_cells = (inputPoints.mapPartitions(lambda partition: count_points_per_cell(partition, LAMBDA)) 
                                  .reduceByKey(lambda x,y : x + y))
          
    # Step B    
    cell_size_dict = non_empty_cells.collectAsMap() # Dictionary with (cell indexes, size)
    

    #Ogni partizione ha elementi con (cell indexes, size), non serve unire dopo per chiave pechè queste sono già state unite prima
    outliers_RDD = (non_empty_cells.flatMap(lambda partition: count_neighborhood(partition, cell_size_dict)))

    outliers = outliers_RDD.collect()
    true_outliers = 0
    uncertain_outliers = 0
    for elem in outliers :          # elem = ((index, index), (cellSize, N3, N7))
        if (elem[1][2] <= M): true_outliers += elem[1][0]
        if (elem[1][2] > M and elem[1] <= M): uncertain_outliers += elem[1][0]
            
    print(f"Number of sure outliers = {true_outliers}")
    print(f"Number of uncertain outliers = {uncertain_outliers}")
    
    ordered_cells_RDD = non_empty_cells.flatMap(invert_key_value).sortByKey()
    #for k in range(min(K, len(nonEmpty_cells))):   
    #   print(f"Cell: {nonEmpty_cells[k][0]}  Size = {nonEmpty_cells[k][1]}")
        
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
    #print(f"Dataset = {data_path.split("\\")[-1]} D={D} M={M} K={K} L={L}")
    print("Number of points = ", numPoints)
    
    
    if numPoints <= 200000 : # EXACT ALGORITHM
        listOfPoints = inputPoints.collect()
        ExactOutliers(listOfPoints, D, M, K)
    
    # APPROXIMATE ALGORITHM
    MRApproxOutliers(inputPoints, D, M, K)

    
if __name__ == "__main__":
    main()