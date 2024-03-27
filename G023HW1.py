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
    

# Find the cell where a point is
def find_cell(point, cell_size):    # cell_size is LAMBDA
    x = point[0]
    y = point[1]
    i = int(x / cell_size) # cast to int to do the floor of the division
    j = int(y / cell_size)
    return (i, j)

# Count the number of points in the same cell
def count_points_per_cell(point, LAMBDA):  

    cell_cord = find_cell(point, LAMBDA)
    print(cell_cord)
    return ((cell_cord[0], cell_cord[1]), 1)


def find_cost_index_list(list_cell_count, target_cord):
    for key_val in list_cell_count:
        if (key_val[0][0] == target_cord[0] and key_val[0][1] == target_cord[1]) :
            return key_val[1]
    
    return 0



# Count how many points are in the neighborhood size x s
def count_in_neighborhood(list_cell_count):
    size_3 = 3
    size_7 = 7
    
    list_refactor = []
    for elem in list_cell_count :  
        neighbors = []
        cost_neighbours_grid3 = 0
        cost_neighbours_grid7 = 0
        for i in range(-(size_3 // 2), size_3 // 2 + 1):
            for j in range(-(size_3 // 2), size_3 // 2 + 1):
                cost_neighbours_grid3 += find_cost_index_list(list_cell_count, (elem[0][0] + i, elem[0][1] + j)) 

        for i in range(-(size_7 // 2), size_7 // 2 + 1):
            for j in range(-(size_7 // 2), size_7 // 2 + 1):
                cost_neighbours_grid7 += find_cost_index_list(list_cell_count, (elem[0][0] + i, elem[0][1] + j))

        list_refactor.append((elem[0], elem[1], cost_neighbours_grid3, cost_neighbours_grid7))
    return list_refactor




def MRApproxOutliers(inputPoints, D, M, K):
    start_time = time.time()
    # Divide R^2 into matrix of size LAMBDA where (i*LAMBDA, j*LAMBDA) are the real coordinates
    LAMBDA = D/2*math.sqrt(2)

   
    # Step A 
    cell_counts = (inputPoints.flatMap(lambda points: count_points_per_cell(points, LAMBDA))
                              .reduceByKey(lambda x,y : x + y))
                              #.sortByKey(True

                     
    # Step B    
    
    cell_counts_list = cell_counts.collect()
    cell_counts_list = count_in_neighborhood(cell_counts_list)


    true_outliers = 0
    uncertain_outliers = 0
    for elem in cell_counts_list :
        if (elem[3] <= M):
            true_outliers += 1
        if (elem[3] > M and elem[2] <= M):
            uncertain_outliers += 1
            
          
    
    print(f"Number of sure outliers = {true_outliers}")
    print(f"Number of uncertain outliers = {uncertain_outliers}")
    
    
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
    print(f"Dataset = {data_path.split("\\")[-1]} D={D} M={M} K={K} L={L}")
    print("Number of points = ", numPoints)
    
    
    if numPoints <= 200000 : # EXACT ALGORITHM
        listOfPoints = inputPoints.collect()
        ExactOutliers(listOfPoints, D, M, K)
    
    # APPROXIMATE ALGORITHM
    MRApproxOutliers(inputPoints, D, M, K)

    
if __name__ == "__main__":
    main()