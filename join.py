import MapReduce
import sys

"""
SELECT * 
FROM Orders, LineItem 
WHERE Order.order_id = LineItem.order_id

in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: order_id
    # value:table attributes
    
    #hardcoded value
    order_id_index = 1


    key = record[order_id_index]
    value = record

    mr.emit_intermediate(key, value)



def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    
    for tuple1 in list_of_values:
      for tuple2 in list_of_values:
        if(tuple1[0] != tuple2[0] and tuple1[0] == 'order'):
          joined_table = tuple1 + tuple2
          mr.emit(joined_table)
          
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
