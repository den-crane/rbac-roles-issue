from clickhouse_driver import Client
import random

TBLNAMEPREFIX="some_table_old_long_name_"
RLNAME="test_long_role_name"
OTHERRLNAME="other_long_role_name"

TBLTEMPLATE="(A Int64) Engine=MergeTree order by A"

clientDefault = Client('localhost', user='default', password='abc')

print("##### Cleaning Stage ######")
clientDefault.execute("drop database if exists test")
clientDefault.execute("drop role if exists %s" % RLNAME)
clientDefault.execute("drop user if exists testuser")

print("##### Init Stage ######")
print(clientDefault.execute('select version(), user()'))
clientDefault.execute("create user if not exists testuser identified by 'abc'")
clientDefault.execute("create role if not exists %s" % RLNAME)
clientDefault.execute("grant %s to testuser" % RLNAME)

clientDefault.execute("create database if not exists test Engine=Atomic")

# create 100 roles, to make chaos
for iter in range(100):
    other_role = OTHERRLNAME + str(iter)
    clientDefault.execute("create role if not exists %s" % other_role)

clientProbe = Client('localhost', user='testuser', password='abc', database='test')
print(clientProbe.execute('select version(), user()'))

print("##### Test Stage ######")

iterations=1000
print("Iterations: %d" % iterations)
print("")

for iter in range(iterations):
  if iter % 100==0: print("Iteration: %s" % iter)

  table_name = "test." + TBLNAMEPREFIX + str(iter)
  clientDefault.execute("create table if not exists %s %s" % (table_name, TBLTEMPLATE))

  # chaos / randomly drop some table from the previous iteration
  rand = random.randint(1,2)
  if rand == 1 and iter > 5:
     tableToDrop = "test." + TBLNAMEPREFIX + str(iter - 3)
     clientDefault.execute("drop table if exists %s" % tableToDrop)

  # chaos / randomly try to read probably not available table
  rand_table_name = "test." + TBLNAMEPREFIX + str(random.randint(1,10))
  try:
     clientProbe.execute("select * from %s"  % rand_table_name)
  except Exception:
    pass

  # check access with default user
  clientDefault.execute("grant select on %s to %s" % (table_name, RLNAME))

  # check access through the role
  clientProbe.execute("select * from %s" % table_name)

  # more chaos / drop some roles
  other_role = OTHERRLNAME + str(random.randint(1,50))
  clientDefault.execute("drop role if exists %s" % other_role)

  # more chaos / grant access to a random role on a random table, most them do not exist
  # it's just to produce errors and create the random chaos
  randtable = "test." + TBLNAMEPREFIX + str(random.randint(1,500))
  other_role = OTHERRLNAME + str(random.randint(1,100))
  try:
    clientDefault.execute("grant select on %s to %s" % (randtable, other_role))
  except Exception:
    pass

  # false positive test / revoke access / check that is no access / grant it back
  if random.randint(1,10) == 1:
    falsePTestPassed = False
    clientDefault.execute("revoke select on %s from %s" % (table_name, RLNAME))
    try:
      clientProbe.execute("select * from %s" % table_name)
    except Exception:
      falsePTestPassed = True
      pass
    if not falsePTestPassed:
      exceptionMessage = "False positive test failed, user has access"
      raise Exception(exceptionMessage)
    clientDefault.execute("grant select on %s to %s" % (table_name, RLNAME))

  # more chaos / drop more tables (drop just accessed table)
  if random.randint(1,5) == 1:
     clientDefault.execute("drop table if exists %s" % table_name)

  # check that default and testuser have an acess to the same number of tables
  cnt1 = clientDefault.execute("select count() from system.tables where database = 'test'")[0][0]
  cnt2 = clientProbe.execute("select count() from system.tables where database = 'test'")[0][0]
  if cnt1 != cnt2:
      exceptionMessage = "Number of available tables is inconsistent: %d, %d" % (cnt1, cnt2)
      raise Exception(exceptionMessage)
