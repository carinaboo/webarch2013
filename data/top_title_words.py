"""Find top 10 most common title words in all pages.

This map-reduce part will take a CSV data file and output tab-seperated lines of

    Word -> count

To run:

    python top_title_words.py anonymous-msweb.data

To store output:

    python top_title_words.py anonymous-msweb.data > top_title_words_unsorted.out

    cat top_title_words_unsorted.out | sort -n -r -k2 > top_title_words_sorted.out

    head -10 top_title_words_sorted.out > top_title_words.out
"""

from mrjob.job import MRJob
from combine_user_visits import csv_readline

class TopTitles(MRJob):

    def mapper(self, line_no, line):
        """Extracts the word from page title"""
        cell = csv_readline(line)
        if cell[0] == 'A':
            title = cell[3]
            for word in title.split():
                yield word, 1
                # What  Key, Value  do we want to output? 
                # key = word, value = 1 occurrence

    def reducer(self, word, occurrences):
        """Sumarizes the visit counts by adding them together."""
        total = sum(occurrences)
            # How do we calculate the total word occurrences from occurrences?
        yield word, total
        	# What  Key, Value  do we want to output?
            # key = word, value = total occurrences

    def mapper_sort(self, word, occurrences):
        """Flips occurrences and word, neg occurrences to sort from greatest to least"""
        yield -1*occurrences, word

    def reducer_sort(self, occurrences, word):
        """Outputs the top 10 words"""
        for i in range(10):
            yield word, -1*occurrences
            # key = word, value = total occurrences
            
    def steps(self):
        return [
            self.mr(mapper=self.mapper,
                    reducer=self.reducer),
            self.mr(mapper=self.mapper_sort,
                    reducer=self.reducer_sort)
        ]
# separate sort after map-reduce
# sort by occurrences
# output first 10

# or do two map-reduces
# reduce 1:  yield -1*total, word
#    map 2:  do nothing, pass along input
# reduce 2:  from 0 < 10: yield word, -1*total

        
if __name__ == '__main__':
    TopTitles.run()
