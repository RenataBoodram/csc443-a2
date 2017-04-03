import MapReduce
import sys

"""
Multiply matrix using MapReduce framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    matrix = record[0]
    i = record[1]
    j = record[2]
    value = record[3]
    if matrix == "a":
        matrix += str(i)
    else:
        matrix += str(j)
    mr.emit_intermediate(matrix, [i,j,value])

def reducer(key, list_of_values):
    # Only perform this while chunking up matrix A
    # (we don't want to do it twice)
    if key[0] == "a":
        # Check keys in the intermediate dict
        for k in mr.intermediate:
            # If we are looking at b, we want to start multiplying
            if k[0] == "b":
                product = 0
                # Get the a_val in the list_of_values
                for a_val in list_of_values:
                    for b_val in mr.intermediate.get(k):
                        # Check if the A-column == B-row 
                        if a_val[1] == b_val[0]: 
                            # Multiply!
                            product += a_val[-1]*b_val[-1]       
                i = int(key[1:])  
                j = int(k[1:])  
                mr.emit((i,j,product))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
