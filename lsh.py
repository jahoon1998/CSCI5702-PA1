# Authors: Jessica Su, Wanzi Zhou, Pratyaksh Sharma, Dylan Liu, Ansh Shukla
# Modified by Evan Stene for CSCI 5702/7702

import numpy as np
import random
import time
import pdb
import unittest
from PIL import Image
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pandas as pd

class LSH:

        # TODO: Implement this
    @staticmethod
    def create_function(dimensions, thresholds):
        """
        Creates a hash function from a list of dimensions and thresholds.
        """
        # functions that compute L K-bit hash keys
        def f(v):
            result = ''
            for i in range(len(dimensions)):
                if int(float(v[dimensions[i]])) >= thresholds[i]:
                    result += '1'
                else:
                    result +='0'
            return result

        return f

    @staticmethod
    def create_functions(L, K, num_dimensions=400, min_threshold=0, max_threshold=255):
        """
        Creates the LSH functions (functions that compute L K-bit hash keys).
        Each function selects k dimensions (i.e. column indices of the image matrix)
        at random, and then chooses a random threshold for each dimension, between 0 and
        255.  For any image, if its value on a given dimension is greater than or equal to
        the randomly chosen threshold, we set that bit to 1.  Each hash function returns
        a length-k bit string of the form "0101010001101001...", and the L hash functions 
        will produce L such bit strings for each image.
        """
        functions = []
        for i in range(L):
            dimensions = np.random.randint(low = 0, 
                                    high = num_dimensions,
                                    size = K)
            thresholds = np.random.randint(low = min_threshold, 
                                    high = max_threshold + 1, 
                                    size = K)
    
            functions.append(LSH.create_function(dimensions, thresholds))
        return functions
    
    def __init__(self, filename, k, L):
        """
        Initializes the LSH object
        filename - name of file containing dataframe to be searched
        k - number of thresholds in each function
        L - number of functions
        """
        # do not edit this function!
        conf = SparkConf().setMaster("local")
        # changed line below to solve "Cannot run multiple SparkContexts at once; 
        # existing SparkContext(app=pyspark-shell, master=local)" error
        self.sc = SparkContext.getOrCreate(conf=conf)
        #0elf.sc = SparkContext.getOrCreate()
        self.k = k
        self.L = L
        self.A = self.load_data(filename)
        functions = LSH.create_functions(self.L, self.k, 400, 0, 255)       
        self.hashed_A = self.hash_data(functions)
    
        '''
        test = self.hashed_A.zipWithIndex().take(2)
        print(type(test))
        for i in test:
            print(i[1])
        temp = sorted(test, key = lambda x: x[1], reverse=True)
        print(temp)
        '''
    # TODO: Implement this
    # works
    def l1(self, u, v):
        """
        Finds the L1 distance between two vectors
        u and v are 1-dimensional Row objects
        """
        sum = 0          
        for x, y in zip(u, v):
            sum += abs(int(float(x)) - int(float(y)))
        return sum
        #raise NotImplementedError

    # TODO: Implement this
    def load_data(self, filename):
        """
        Loads the data into a spark DataFrame, where each row corresponds to
        an image patch -- this step is sort of slow.
        Each row in the data is an image, and there are 400 columns.
        """
        sqlContext = SQLContext(self.sc)
        #df = sqlContext.read.csv("D:/BigDataMining/Lab1/" + filename, header=None)
        df = sqlContext.read.format('com.databricks.spark.csv')\
        .options(header='false', inferschema='false').load("D:/BigDataMining/Lab1/" + filename)
        return df

    # TODO: Implement this
    def hash_vector(self, v, functions):
        """
        Hashes an individual vector (i.e. image).  This produces an array with L
        entries, where each entry is a string of k bits.
        """
        #self.functions = self.create_functions(400, 0 , 255)
        array = []
        for i in range(len(functions)):
            array.append(functions[i](v))
        return array
        # you will need to use self.functions for this method
        #raise NotImplementedError

    # TODO: Implement this
    def hash_data(self, functions):
        """
        Hashes the data in A, where each row is a datapoint, using the L
        functions in 'self.functions'
        """
        # you will need to use self.A for this method
        #raise NotImplementedError
        
        # convert dataframe to RDD and do other calculation
        # skiping hash_vector? maybe??
        #return self.A.rdd.map(lambda row: self.hash_vector[row]);
        
        return pd.DataFrame(self.A.rdd.map(lambda row: LSH.hash_vector(LSH, row[:], functions)).collect())
       
        
        '''
        array = []
        for i in range(1, self.A.count() + 1):
            array.append(self.hash_vector(np.array(self.A.take(i)[0])))
        
        return array
        '''
    # TODO: Implement this
    def hash_bucket(self, hashed_point, compared_point):
        # return number of times hashed point and compared point are in the
        # same bucket
        result = 0
        for i in range(len(hashed_point)):
            temp = LSH.l1(self, hashed_point[i], compared_point[i])
            if temp == 0:
                result += 1
                
        #raise NotImplementedErro
        return result
    
    # TODO: Implement this
    # hashed_point hashed value of the image in query_index
    def get_candidates(self, query_index, hashed_point, num_candidates):
        """
        Retrieve all of the points that hash to one of the same buckets 
        as the query point.  Do not do any random sampling (unlike what the first
        part of this problem prescribes).
        Don't retrieve a point if it is the same point as the query point.
        """
        '''
        cand_list = map(lambda r: (r, np.array_equal(np.array(self.hashed_A.iloc[query_index-1]), 
                           np.array(self.hashed_A.iloc[r]))) if r != query_index-1 else (r, False), 
                                                            range(len(self.hashed_A)))
        '''
        # you will need to use self.hashed_A for this method
        #raise NotImplementedError
        cand_list = map(lambda r: (r, LSH.hash_bucket(LSH, np.array(self.hashed_A.iloc[query_index-1]), 
                           np.array(self.hashed_A.iloc[r]))) if r != query_index-1 else (r, 0), 
                                                            range(len(self.hashed_A)))

        '''
        temp = self.hashed_A.map(lambda row: LSH.hash_bucket(LSH, hashed_point[:], row[:]))
        withIndices = temp.zipWithIndex().collect()
        # set the query point's number of common hash bucket hit to 0
        # to eliminate from candidates
        withIndices[query_index - 1] = (0, query_index-1)
        '''
        # ignore element in the first position 
        # image in the first position is hashed_point itself 
        sorted_cand = sorted(cand_list, key = lambda x : x[1], reverse=True)[:num_candidates]
        '''
        candList_idx = []
        for i in candlist:
            candList_idx.append(i[1])
        '''
        return [x[0] for x in sorted_cand]         

        
    # TODO: Implement this
    def lsh_search(self, dataFrame, query_index, num_neighbors):
        """
        Run the entire LSH algorithm
        """
        num_cand_neighbors = 10
        hashed_point = np.array(self.hashed_A.iloc[query_index-1]) 
        # start time
        start_time = time.time()
        candidate_row_nums = self.get_candidates(query_index, hashed_point, num_cand_neighbors)
        # record the time
     
        distances = map(lambda r: (r, LSH.l1(LSH, np.array(dataFrame.iloc[r]), 
                                             np.array(dataFrame.iloc[query_index -1]))), candidate_row_nums)
        best_neighbors = sorted(distances, key=lambda x: x[1])[:num_neighbors]
        # end time
        exec_time = time.time() - start_time
        print("LSH Search")
        print("--- %s seconds ---" % (exec_time))
        return [x[0] for x in best_neighbors]
        #raise NotImplementedError

# "D:/BigDataMining/Lab1/LSH_results"
# Plots images at the specified rows and saves them each to files.
def plot(df, row_nums, base_filename):
    for row_num in row_nums:
        patch = np.reshape(np.array(df.iloc[row_num]), [20, 20])
        im = Image.fromarray(patch)
        if im.mode != 'RGB':
            im = im.convert('RGB')
        im.save(base_filename + "-" + str(row_num) + ".png")

# Finds the nearest neighbors to a given vector, using linear search.
# TODO: Implement this
def linear_search(df, query_index, num_neighbors):
    #raise NotImplementedError
    big_number = 99999 
    #start time
    start_time = time.time()
    # calculate distance
    # if index of row is equal to query_index, then set the distance to big number
    # to eliminate itself from best neighbors
    distances = map(lambda r: (r, LSH.l1(LSH, np.array(df.iloc[r]), 
                np.array(df.iloc[query_index -1]))) if r != query_index-1 else (r, big_number), range(len(df)))
    
    #distances[query_index -1] =  (query_index-1, float("inf")) 
    best_neighbors = sorted(distances, key=lambda x: x[1])[:num_neighbors]
    # end time
    exec_time = time.time() - start_time
    print("Linear Search")
    print("--- %s seconds ---" % (exec_time))
    return [x[0] for x in best_neighbors]

# Write a function that computes the error measure
# TODO: Implement this
def lsh_error(df, query_index, LSH_list, Linear_list):
    error = 0
    query_i = 0
    for LSH_row, Linear_row in zip(LSH_list, Linear_list):
        LSH_distance_sum = sum(LSH.l1(LSH, np.array(df.iloc[i]),
                                                    np.array(df.iloc[query_index[query_i] - 1])) for i in LSH_row)
        Linear_distance_sum = sum(LSH.l1(LSH, np.array(df.iloc[i]),
                                            np.array(df.iloc[query_index[query_i] - 1])) for i in Linear_row)
        temp = LSH_distance_sum / Linear_distance_sum
        error += temp
        query_i += 1
    return error / len(LSH_list)
    #raise NotImplementedError

def readPanda(filename):
    df = pd.read_csv("D:/BigDataMining/Lab1/" + filename, header=None)
    return df
    
    
#### TESTS #####

class TestLSH(unittest.TestCase):
    def test_l1(self):
        u = np.array([1, 2, 3, 4])
        v = np.array([2, 3, 2, 3])
        self.assertEqual(self.l1(u, v), 4)

    def test_hash_data(self):
        f1 = lambda v: sum(v)
        f2 = lambda v: sum([x * x for x in v])
        A = np.array([[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f1(A[0,:]), 6)
        self.assertEqual(f2(A[0,:]), 14)
        
        functions = [f1, f2]
        self.hash_vector(functions, A[0, :])
        self.assertTrue(np.array_equal(self.hash_vector(functions, A[0, :]), np.array([6, 14])))
        self.assertTrue(np.array_equal(self.hash_data(functions, A), np.array([[6, 14], [15, 77]])))
        print("DONE")
    ### TODO: Write your tests here (they won't be graded, 
    ### but you may find them helpful)
    def l1(self, u, v):
        """
        Finds the L1 distance between two vectors
        u and v are 1-dimensional Row objects
        """
        sum = 0          
        for i in range(0, len(u)):
            sum += abs(u[i] - v[i])
        return sum
        #raise NotImplementedError

    def hash_vector(self, functions, v):
        """
        Hashes an individual vector (i.e. image).  This produces an array with L
        entries, where each entry is a string of k bits.
        """
       # self.functions = self.create_functions(400, 0 , 255)
        array = []
        for i in range(len(functions)):
            array.append(functions[i](v))
        return array

    def hash_data(self, functions, A):
        """
        Hashes the data in A, where each row is a datapoint, using the L
        functions in 'self.functions'
        """
        # you will need to use self.A for this method
        #raise NotImplementedError
        array = []
        for i in range(len(A)):
            array.append(self.hash_vector(functions, A[i]))
        
        return array
    
if __name__ == '__main__':
#    unittest.main() ### TODO: Uncomment this to run tests
    # create an LSH object using lsh = LSH(k=16, L=10)
    """
    Your code here
    """
    k = 10
    L = 24
    lsh = LSH("patches.csv", k, L)
    df = readPanda("patches.csv")
    print("L: {}, k: {}".format(L, k))
    query_index = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    LSH_indicies = []
    Linear_indicies = []
    for i in query_index:
        print("Query Index is {}".format(i))
        LSH_best_neighbors_rows = lsh.lsh_search(df,i, 3)
        LSH_indicies.append(LSH_best_neighbors_rows)
        LSH_filename = str(i) + "LSH_Neighbor_";
        plot(df, LSH_best_neighbors_rows, LSH_filename)
        
        Linear_best_neighbors_rows = linear_search(df, i, 3)
        Linear_indicies.append(Linear_best_neighbors_rows)
        Linear_filename = str(i) + "Linear_Neighbor_";
        plot(df, Linear_best_neighbors_rows, Linear_filename)
        print("---------------------------------")
        
    print(LSH_indicies)
    print(Linear_indicies)
    error = lsh_error(df, query_index, LSH_indicies, Linear_indicies)
    print("error is {}".format(error))
    lsh.sc.stop()