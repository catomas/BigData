from mrjob.job import MRJob

class salario(MRJob):

  def mapper(self, _, line):
    idemp, sector, salary, year = line.split(',')
    try:
      salary = float(salary)
    except ValueError:
      pass
    else:
      yield idemp, salary
    
      
  def reducer (self, idemp, salary):
    sumaSalarios = 0
    cont = 0
    for sal in salary:
      sumaSalarios = sumaSalarios + sal
      cont = cont +1   
    yield idemp, sumaSalarios / cont

if __name__ == '__main__':
  salario.run()