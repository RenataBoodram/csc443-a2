import MapReduce
import sys

"""
semetric friend in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: person_id
    # value: the relationship [a, b]
    keyA = record[0]
    keyB = record[1]
    value = record

    # by inserting on both people by the reduce phase
    # you will end up with all the people the person [key]
    # is following or being followed by, allowing for easy
    # reduce
    mr.emit_intermediate(keyA, value)
    mr.emit_intermediate(keyB, value)


def reducer(key, list_of_values):
    # key: person_id
    # list_of_value: list of follower/followee relationships

    count = 0
    # use enumarate for loop since it allows you to utilize indexs [i, j]
    # to prevent double counting
    for i,relA in enumerate(list_of_values):
      for j,relB in enumerate(list_of_values):
        if(is_symetric_friends(relA, relB) and i > j):
          count+=1

    #this prevents people with 0 friends being printed
    # if(count ==0):
    #   return
    mr.emit((key, count))


def is_symetric_friends(relA, relB):

  if(relA[0] == relB[1] and relA[1] == relB[0]):
    # print(a + "==:== " + b )
    return True
  else:
    return False

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
