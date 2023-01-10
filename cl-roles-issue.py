from clickhouse_driver import Client
import random

print("##### Init Stage ######")

clientDefault = Client('localhost', user='default', password='abc')

print(clientDefault.execute('select version(), user()'))

clientDefault.execute("create user if not exists testuser identified by 'abc'")
clientDefault.execute("create role if not exists testrole")
clientDefault.execute("grant testrole to testuser")
#clientDefault.execute("drop database if exists test")
clientDefault.execute("create database if not exists test Engine=Atomic")

# create 100 roles
for iter in range(100):
    clientDefault.execute("create role if not exists testrole%s" % iter)

clientProbe = Client('localhost', user='testuser', password='abc', database='test')
print(clientProbe.execute('select version(), user()'))

print("##### Test Stage ######")

iterations=10000
print("Iterations: %d" % iterations)

for iter in range(iterations):
  if iter % 100==0:
      print("iteration: %s" % iter)

  clientDefault.execute("create table if not exists test.test%s(a Int64) Engine=MergeTree order by a as select 1" % iter)

  # chaos / randomly drop some table from the previous iteration
  rand = random.randint(1,2)
  if rand == 1 and iter > 5:
     tableToDrop = iter - 3
     clientDefault.execute("drop table if exists test.test%s" % tableToDrop)

  clientDefault.execute("grant select on test.test%s to testrole" % iter)
  #rand = random.randint(1,50)
  #if rand == 1:
  #     clientDefault.execute("revoke select on test.test%s from testrole" % iter)

  # check access through the role
  clientProbe.execute("select * from test.test%s"  % iter)

  # more chaos / drop some roles
  rand = random.randint(1,50)
  clientDefault.execute("drop role if exists testrole%s" % rand)

  # more chaos / drop more tables (drop just accessed table)
  rand = random.randint(1,5)
  if rand == 1:
     tableToDrop = iter
     clientDefault.execute("drop table if exists test.test%s" % tableToDrop)

  # more chaos / grant access to acrandom role on a random table, most them do not exist
  # it's just to produce errors and create the random chaos
  randtable = random.randint(1,500)
  randrole = random.randint(1,100)
  try:
    clientDefault.execute("grant select on test.test%d to testrole%d" % (randtable, randrole))
  except Exception:
    pass

  # check that default and testuser have an acess to the same number of tables
  cnt1 = clientDefault.execute("select count() from system.tables where database = 'test'")[0][0]
  cnt2 = clientProbe.execute("select count() from system.tables where database = 'test'")[0][0]
  if cnt1 != cnt2:
      exceptionMessage = "Number of available tables is inconsistent: %d, %d" % (cnt1, cnt2)
      raise Exception(exceptionMessage)

#print("##### Cleaning Stage ######")
#clientDefault.execute("drop database if exists test")
#clientDefault.execute("drop role if exists testrole")
#clientDefault.execute("drop user if exists testuser")
