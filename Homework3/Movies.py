from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMovies(MRJob):
    def steps(self):
        return[
            MRStep(mapper = self.mapper_get_user_ratings,
                   reducer = self.reducer_count_user_ratings)
        ]

    def mapper_get_user_ratings(self, _, line):
        (userID, moveID, rating, timestamp) = line.split(',')
        yield userID, 1

    def reducer_count_user_ratings(self, userID, counts):
        yield userID, sum(counts)


if __name__ == '__main__':
    MRMovies.run()


