from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")


class MRWordCount(MRJob):

    def mapper(self, _, line):
        range = 'abcdefghikjlmn'
        for word in WORD_RE.findall(line):
            if any(word.startswith(x) for x in range):
                output = 'a_to_n'
            else:
                output = 'other'
            yield output, 1

    def combiner(self, output, counts):
        yield output, sum(counts)

    def reducer(self, output, counts):
        yield output, sum(counts)


if __name__ == '__main__':
    MRWordCount.run()


