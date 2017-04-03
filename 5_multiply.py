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
    matrix += str(i)
    mr.emit_intermediate(matrix, [i,j,value])

def reducer(key, list_of_values):
    # Only perform this while chunking up matrix A
    # (we don't want to do it twice)
    product_dict = {} 
    
    if key[0] == "a": 
        print(key)
        a_vals = mr.intermediate.get(key)
        for key2 in mr.intermediate:
            if key2[0] == "b":
                print(key2)
                b_vals = mr.intermediate.get(key2)
                # j == i
                print("COMPARE:", a_vals,b_vals)
                for a in a_vals:
                    for b in b_vals:
                        if a[1] == b[0]:
                            product = a[-1]*b[-1]
                            print(product)
                            # Insert into dictionary
                            if (a[0],b[1]) in product_dict:
                                product_dict[(a[0],b[1])] += product
                            else:
                                product_dict[(a[0],b[1])] = product
    for pkey in product_dict:
        mr.emit(pkey + (product_dict[pkey],))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
