from mrjob.job import MRJob
from mrjob.step import MRStep
import re

word_re = re.compile(r"([A-Za-z]+['-]{1}[A-Za-z]+|[A-Za-z]+)") 
#check hyphenated or apostrophe words, or normal words

class MRWordCount(MRJob):
    
    def mapper(self, _, line):
        for word in word_re.findall(line):
            yield (word.lower(), 1)
            
    def combiner(self, word, counts):
        yield (word, sum(counts))
        
    def reducer(self, word, counts):
         yield (word, sum(counts))

if __name__ == '__main__':
    MRWordCount.run()