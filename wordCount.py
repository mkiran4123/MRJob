
from mrjob.job import MRJob
from mrjob.step import MRStep
from google.cloud import storage
from google.cloud import logging
import time
import re

WORD_RE = re.compile(r"[\w']+")


class MRWordFreqSort(MRJob):

    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)

    def reducer_sort_word_freq(self, _, word_count_pairs):
        for count, key in sorted(word_count_pairs, reverse=True):
            yield (int(count), key)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_sort_word_freq)
        ]


if __name__ == '__main__':
    start_time = time.time()
    MRWordFreqSort.run()
    print("--- %s seconds ---" % (time.time() - start_time))
