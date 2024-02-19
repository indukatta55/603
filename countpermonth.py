from mrjob.job import MRJob
import csv

class CountPerMonth(MRJob):

    def mapper(self, _, line):
        # Parse the input line as a CSV row
        row = next(csv.reader([line]))
        # Extract the date field
        date = row[2]  # Assuming the date is the third field (index 2)
        # Extract the year and month
        year_month = date[:7]  # Extract year and month
        # Emit key-value pair with year-month as key and count 1 as value
        yield year_month, 1

    def reducer(self, year_month, counts):
        # Sum up the counts for each year-month
        total_count = sum(counts)
        # Yield the year-month and total count
        yield year_month, total_count

if __name__ == '__main__':
    CountPerMonth.run()
