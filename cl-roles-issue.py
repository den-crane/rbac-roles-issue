from clickhouse_driver import Client
import random

TBLNAMEPREFIX="some_table_old_long_name_"

clientDefault = Client('localhost', user='default', password='abc')

print("##### Cleaning Stage ######")
clientDefault.execute("drop database if exists test")
clientDefault.execute("drop role if exists test_long_role_name")
clientDefault.execute("drop user if exists testuser")

print("##### Init Stage ######")
print(clientDefault.execute('select version(), user()'))
clientDefault.execute("create user if not exists testuser identified by 'abc'")
clientDefault.execute("create role if not exists test_long_role_name")
clientDefault.execute("grant test_long_role_name to testuser")


clientDefault.execute("create database if not exists test Engine=Atomic")

# create 100 roles
for iter in range(100):
    clientDefault.execute("create role if not exists test_long_role_name%s" % iter)

clientProbe = Client('localhost', user='testuser', password='abc', database='test')
print(clientProbe.execute('select version(), user()'))

print("##### Test Stage ######")

iterations=1000
print("Iterations: %d" % iterations)

for iter in range(iterations):
  if iter % 100==0:
      print("iteration: %s" % iter)

  table_name = "test." + TBLNAMEPREFIX + str(iter)
  clientDefault.execute("create table if not exists %s(a Int64) Engine=MergeTree order by a as select 1" % table_name)

  # chaos / randomly drop some table from the previous iteration
  rand = random.randint(1,2)
  if rand == 1 and iter > 5:
     tableToDrop = "test." + TBLNAMEPREFIX + str(iter - 3)
     clientDefault.execute("drop table if exists %s" % tableToDrop)

  # chaos / randomly try to read probably not available table
  rand = random.randint(1,10)
  try:
     clientProbe.execute("select * from test.test%s"  % rand)
  except Exception:
    pass

  clientDefault.execute("grant select on %s to test_long_role_name" % table_name)

  # check access through the role
  clientProbe.execute("select * from %s" % table_name)

  # more chaos / drop some roles
  rand = random.randint(1,50)
  clientDefault.execute("drop role if exists test_long_role_name%s" % rand)

  # more chaos / grant access to a random role on a random table, most them do not exist
  # it's just to produce errors and create the random chaos
  randtable = "test." + TBLNAMEPREFIX + str(random.randint(1,500))
  randrole = random.randint(1,100)
  try:
    clientDefault.execute("grant select on %s to test_long_role_name%d" % (randtable, randrole))
  except Exception:
    pass

  # false positive test / revoke access / check that no access / grant it back
  falsePTestRand = random.randint(1,10)
  falsePTestPassed = False
  if falsePTestRand == 1:
    clientDefault.execute("revoke select on %s from test_long_role_name" % table_name)
    try:
      clientProbe.execute("select * from %s" % table_name)
    except Exception:
      falsePTestPassed = True
      pass
    if not falsePTestPassed:
      exceptionMessage = "False positive test failed, user has access"
      raise Exception(exceptionMessage)
    clientDefault.execute("grant select on %s to test_long_role_name" % table_name)

  # more chaos / drop more tables (drop just accessed table)
  rand = random.randint(1,5)
  if rand == 1:
     tableToDrop = table_name
     clientDefault.execute("drop table if exists %s" % tableToDrop)

  # check that default and testuser have an acess to the same number of tables
  cnt1 = clientDefault.execute("select count() from system.tables where database = 'test'")[0][0]
  cnt2 = clientProbe.execute("select count() from system.tables where database = 'test'")[0][0]
  if cnt1 != cnt2:
      exceptionMessage = "Number of available tables is inconsistent: %d, %d" % (cnt1, cnt2)
      raise Exception(exceptionMessage)

#print("##### Cleaning Stage ######")
#clientDefault.execute("drop database if exists test")
#clientDefault.execute("drop role if exists test_long_role_name")
#clientDefault.execute("drop user if exists testuser")
