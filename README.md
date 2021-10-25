# svar_sqlite3

The svar interface of sqlite3.

## Compile

```
mkdir build&&cd build
cmake ..&&sudo make install
```

## Usage 


```
pip3 install git+https://github.com/zdzhaoyong/Svar@f73504b815e7a7b24dd4c660df148e9d99d71d6d
cd python
python3 test.py
```

Run with python3:

```
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
```
