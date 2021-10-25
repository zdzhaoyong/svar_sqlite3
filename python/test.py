import svar
import time

sql = svar.load('svar_sqlite3')

database = sql.Database('test.db')

database.execute('create table if not exists nameage(name text PRIMARY KEY, age integer)')
database.execute('replace into nameage values("zhaoyong",29)')
cur=database.execute('replace into nameage values(?,?)',('wl',30))

t1=time.time()
for i in range(0,10):
  cur.execute((str(i),i))

count=database.execute('select count(*) from nameage where age<10').fetchone()[0]
print(time.time()-t1," to insert", count,"items")

print(database.execute('select * from nameage').fetchall())
