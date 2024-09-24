from mrjob.job import MRJob

class MRSalaries(MRJob):

    def mapper(self, _, line):
        (name,jobTitle,agencyID,agency,hireDate,annualSalary,grossPay) = line.split('\t')
        annualSalary = float(annualSalary)
        if annualSalary >= 100000.00:
            salaryClass = 'High'
        elif annualSalary >= 50000.00:
            salaryClass = 'Medium'
        else:
            salaryClass = 'Low'
        yield salaryClass, 1

    def combiner(self, salaryClass, counts):
        yield salaryClass, sum(counts)

    def reducer(self, salaryClass, counts):
        yield salaryClass, sum(counts)


if __name__ == '__main__':
    MRSalaries.run()


