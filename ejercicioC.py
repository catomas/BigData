from mrjob.job import MRJob

class salario(MRJob):

  def mapper(self, _, line):
    idemp, sector, salary, year = line.split(',')
   
    yield (idemp, (sector, 1))
    
      
  def reducer (self, idemp, sector):
    sumaSector = 0

    for sec, cont in sector:
      sumaSector = sumaSector + cont
       
    yield idemp, sumaSector

if __name__ == '__main__':
  salario.run()