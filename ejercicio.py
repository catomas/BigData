from mrjob.job import MRJob

class salario(MRJob):

  def mapper(self, _, line):
    idemp, sector, salary, year = line.split(',')
    try:
      salary = float(salary)
    except ValueError:
      pass
    else:
      yield sector, salary
      
  def reducer (self, sececon, salarios):
    sumaSalarios = 0
    cont = 0
    for sal in salarios:
      sum_salaries = sum_salaries + sal
      cont = cont +1
        
    yield sececon, sum_salaries / cont

if __name__ == '__main__':
  salario.run()