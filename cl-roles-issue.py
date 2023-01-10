from clickhouse_driver import Client
import random

print("##### Init Stage ######")

clientDefault = Client('localhost', user='default', password='abc')

print(clientDefault.execute('select version(), user()'))

clientDefault.execute("create user if not exists testuser identified by 'abc'")
clientDefault.execute("create role if not exists testrole")
clientDefault.execute("grant testrole to testuser")
clientDefault.execute("drop database if exists test")
clientDefault.execute("create database test Engine=Memory")

clientProbe = Client('localhost', user='testuser', password='abc', database='test')

print(clientProbe.execute('select version(), user()'))

print("##### Test Stage ######")
for x in range(10000):
  if x % 100==0:
      print("iteration: %s" % x)
  clientDefault.execute("create table if not exists test.test%s(a Int64) Engine=Memory" % x)
  rand = random.randint(1,2)
  if rand == 1 and x > 5:
     tableToDrop = x - 3
     clientDefault.execute("drop table if exists test.test%s" % tableToDrop)
     #print('dropped %s' % tableToDrop )
  clientDefault.execute("grant select on test.test%s to testuser" % x)
  #print (clientProbe.execute("select count() from test.test%s"  % x))

print("##### Cleaning Stage ######")
clientDefault.execute("drop database if exists test")
clientDefault.execute("drop role if exists testrole")
clientDefault.execute("drop user if exists testuser")
