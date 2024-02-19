from mrjob.job import MRJob
import csv

class AvgWordCount(MRJob):

    def mapper(self, _, line):
        # Parse the input line as a CSV row
        row = next(csv.reader([line]))
        # Extract text field
        text = row[5]
        # Split text into words and emit the word count along with 1
        yield "word_count", len(text.split())

    def reducer(self, key, values):
        total_words = 0
        total_reviews = 0
        # Iterate through the word counts and count the total number of words
        # Also count the total number of reviews
        for word_count in values:
            total_words += word_count
            total_reviews += 1
        # Calculate the average number of words per review
        average_words_per_review = total_words / total_reviews
        yield "average", average_words_per_review

if __name__ == '__main__':
    AvgWordCount.run()
