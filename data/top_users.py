"""Find Users with more than 20 visits.

This program will take a CSV data file and output tab-seperated lines of

    User -> number of visits

To run:

    python top_users.py user-visits_msweb.data

To store output:

    python top_users.py user-visits_msweb.data > top_users.out
"""

from mrjob.job import MRJob
from combine_user_visits import csv_readline

class TopUsers(MRJob):

    def mapper(self, line_no, line):
        """Extracts the User that visited a page"""
        cell = csv_readline(line)
        if cell[0] == 'V':
            yield cell[3], 1
                  # What  Key, Value  do we want to output? 
                  # key = user, value = 1 visit

    def reducer(self, user, visit_counts):
        """Sumarizes the visit counts by adding them together by user.  If total visits
        is more than 20, yield the results"""
        total = sum(visit_counts)
                # How do we calculate the total visits from the visit_counts?
        if total > 20:
            yield user, total
                  # What  Key, Value  do we want to output?
                  # key = user, value = total visits (if over 20)
        
if __name__ == '__main__':
    TopUsers.run()
