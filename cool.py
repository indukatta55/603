from mrjob.job import MRJob
import csv

class Cool(MRJob):

    def mapper(self, _, line):
        # Parse the input line as a CSV row
        row = next(csv.reader([line]))
        coolness, stars = int(row[8]), int(row[4])  # Assuming coolness is the 9th field and stars is the 5th field
        if coolness != 0 and stars != "stars":
            yield None, (coolness, stars)

    def reducer(self, _, values):
        total_coolness, total_stars, count = 0, 0, 0
        for coolness, stars in values:
            total_coolness += coolness
            total_stars += stars
            count += 1
        if count > 0:
            average_coolness = total_coolness / count
            yield None, average_coolness

if __name__ == '__main__':
    Cool.run()
