from clickhouse_driver import Client
import random
import _thread


TBLNAMEPREFIX="some_table_old_long_name_"
RLNAME="test_long_role_name"
OTHERRLNAME="other_long_role_name"
OTHERRLNUMB=100
USERNAME="testuser_"
USERNUMB=10

TBLTEMPLATE="(A Int64) Engine=MergeTree order by A as select number from numbers(10)"


# try to block random user with sleep or with read from 10th table
# initially table10 is available (deliberatly)
def probe_user_from_thread():
   count = 0
   while True:
      conn = None
      count += 1
      try:
        thread_user = USERNAME + str( random.randint(0,USERNUMB-1) )
        #print("thread: %d, user: %s" % (count, thread_user))
        conn = Client('localhost', user = thread_user, password='abc', database='test')
        conn.execute("select *, sleep(.3) from %s" % TBLNAMEPREFIX+str(1))
        conn.execute("select sleep(.2)")
      except Exception as err:
        #print('Handling run-time error:')
        pass


default = Client('localhost', user='default', password='abc')

print("##### Cleaning Stage ######")
default.execute("drop database if exists test")
default.execute("drop role if exists %s" % RLNAME)
default.execute("drop user if exists testuser")
for iter in range(USERNUMB):
  username = USERNAME + str(iter)
  default.execute("drop user if exists %s" % username)
for iter in range(OTHERRLNUMB):
  other_role = OTHERRLNAME + str(iter)
  default.execute("drop role if exists %s" % other_role)


print("##### Init Stage ######")
print(default.execute('select version(), user()'))

default.execute("create role if not exists %s" % RLNAME)

# create N users and grant them role
for iter in range(USERNUMB):
  username = USERNAME + str(iter)
  default.execute("create user if not exists %s identified by 'abc'" % username)
  default.execute("grant %s to %s" % (RLNAME, username))

default.execute("create database if not exists test Engine=Atomic")

# create 100 roles, to make chaos
for iter in range(100):
  other_role = OTHERRLNAME + str(iter)
  default.execute("create role if not exists %s" % other_role)

probe=[]
for iter in range(USERNUMB):
  username = USERNAME + str(iter)
  conn = Client('localhost', user=username, password='abc', database='test')
  probe.append(conn)
  print(conn.execute('select version(), user()'))

print("##### Test Stage ######")

iterations=1000
print("Iterations: %d" % iterations)
print("")

_thread.start_new_thread( probe_user_from_thread, () )

for iter in range(iterations):
  if iter % 100 == 99: print("Iteration: %s" % iter)

  table_name = "test." + TBLNAMEPREFIX + str(iter)
  default.execute("create table if not exists %s %s" % (table_name, TBLTEMPLATE))

  # chaos / randomly drop some table from the previous iteration
  if random.randint(1,2) == 1 and iter > 5:
    table_to_drop = "test." + TBLNAMEPREFIX + str(iter - 3)
    default.execute("drop table if exists %s" % table_to_drop)

  # chaos / randomly try to read probably not available table
  rand_table = "test." + TBLNAMEPREFIX + str(random.randint(1,10))
  try:
    probe[random.randint(0,USERNUMB-1)].execute("select * from %s"  % rand_table)
  except Exception:
    pass

  # grant access to the role
  default.execute("grant select on %s to %s" % (table_name, RLNAME))

  # check access through the role
  probe[random.randint(0,USERNUMB-1)].execute("select * from %s" % table_name)

  # more chaos / drop some other roles
  other_role = OTHERRLNAME + str(random.randint(1,50))
  default.execute("drop role if exists %s" % other_role)

  # more chaos / grant access to a random role on a random table, most them do not exist
  # it's just to produce errors and create the random chaos
  rand_table = "test." + TBLNAMEPREFIX + str(random.randint(1,500))
  rand_role = OTHERRLNAME + str(random.randint(1,100))
  try:
    default.execute("grant select on %s to %s" % (rand_table, rand_role))
  except Exception:
    pass

  # false positive test / revoke access / check that it fails / grant it back
  if random.randint(1,10) == 1:
    false_ptest_passed = False
    default.execute("revoke select on %s from %s" % (table_name, RLNAME))
    try:
      probe[random.randint(0,USERNUMB-1)].execute("select * from %s" % table_name)
    except Exception:
      false_ptest_passed = True
      pass
    if not false_ptest_passed:
      emessage = "False positive test failed, user has access"
      raise Exception(emessage)
    default.execute("grant select on %s to %s" % (table_name, RLNAME))

  # more chaos / drop more tables (drop just accessed table)
  if random.randint(1,5) == 1:
    default.execute("drop table if exists %s" % table_name)

  # check that default and testuser have an acess to the same number of tables
  cnt1 = default.execute("select count() from system.tables where database = 'test'")[0][0]
  for iter in range(USERNUMB):
    cnt2 = probe[iter].execute("select count() from system.tables where database = 'test'")[0][0]
    if cnt1 != cnt2:
      emessage = "Number of available tables is inconsistent: %d, %d; connection: %d" % (cnt1, cnt2, iter)
      raise Exception(emessage)
