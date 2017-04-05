import MapReduce
import sys

"""
Cleaning DNA sequences in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    # key = record[0]
    # value = record[1]
    # mr.emit_intermediate(key, value)

    # key: uncleanedDNA
    # value: last10Nucleotides
    key = record[1][:-10]
    value = record[1][-10:]
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # # key: word
    # # value: list of occurrence counts
     
    # lastTenNucleotides = ''.join(map(str, list_of_values))[-10:]
    # uncleanedDNA = ''.join(map(str, list_of_values))

    # cleanedDNA = uncleanedDNA.replace(lastTenNucleotides, "")    
    # mr.emit(cleanedDNA)

    #no work to be done in reducer since non unique DNA sequences will go into the same hash key
    mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
