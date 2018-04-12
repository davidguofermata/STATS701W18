from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import math
import functools

# mean sum of current numbers/current number of samples
# variance sum current numbers, then square. divide by number of samples
# also use var = e(x^2)-e(x)^2

# so map sum of current numbers

# map square of sums, take mean of this

# var is 

class MRSummaryStats(MRJob):
        
    def steps(self):
         return [
             MRStep(mapper = self.mapper,
		     combiner = self.combiner,
                   reducer = self.reducer),
             MRStep(reducer = self.reducer_div)
	]

    def mapper(self, _, line):
        yield int(line.split()[0]), float(line.split()[1])

    def combiner(self, label, val):
        for v in val:
            yield label, (1, v, v**2)
    
    def reducer(self, label, val):
        yield label, functools.reduce(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]), val)

    def reducer_div(self, label, val):
        for v in val:
            yield label, (v[0], v[1]/v[0], v[2]/v[0]-(v[1]/v[0])**2)

if __name__ == '__main__':
    MRSummaryStats.run()