# This is a sample Python script
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import atexit
import sqlite3
import _sqlite3
import sys


class Hat:
    def __init__(self, id, topping, supllier, quantity):
        self.id = id
        self.topping = topping
        self.supplier = supllier
        self.quantity = quantity


class Supplier:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Order:
    def __init__(self, id, location, hat):
        self.id = id
        self.location = location
        self.hat = hat


class _Hats:

    def __init__(self, con):
        self._con = con

    def find(self, top):
        c = self._con.cursor()
        c.execute("""
        SELECT id , supplier FROM hats WHERE topping=?
        """, [top])

        toReturn = c.fetchall()
        min = 50000
        index = -1
        for tu in toReturn:
            if tu[1] < min:
                min = tu[1]
                index = tu[0]
        return index

    def insert(self, hat):
        self._con.execute("""
        INSERT INTO hats (id,topping,supplier,quantity) VALUES (?,?,?,?)
        """, [hat.id, hat.topping, hat.supplier, hat.quantity])

    def updateInBase(self, hatOrderedId):
        self._con.execute("""
        UPDATE hats 
        SET quantity = quantity-1
        where id =?
        """, [hatOrderedId])

        c = self._con.cursor()
        value = c.execute("""
        SELECT quantity FROM hats WHERE id=?
        """, [hatOrderedId]).fetchall()
        if value[0][0] == 0:
            self._con.execute("""
            DELETE FROM hats WHERE id=?
            """, [hatOrderedId])

    def print(self):
        c = self._con.cursor()
        toPrint = c.execute("""
        SELECT * FROM hats
        """).fetchall()
        print(toPrint)


class _Suppliers:
    def __init__(self, con):
        self._con = con

    def insert(self, supplier):
        self._con.execute("""
        INSERT INTO suppliers (id , name) VALUES (?,?)
        """, [supplier.id, supplier.name])


class _Orders:
    def __init__(self, con):
        self._con = con

    def insert(self, order):
        self._con.execute("""
        INSERT INTO orders (id , location , hat) VALUES (?,?,?)
        """, [order.id, order.location, order.hat])

    def print(self):
        c = self._con.cursor()
        toPrint = c.execute("""
               SELECT * FROM orders
               """).fetchall()
        print(toPrint)


class _Repository:
    def __init__(self):
        self._con = sqlite3.connect(sys.argv[4])
        self.hats = _Hats(self._con)
        self.suppliers = _Suppliers(self._con)
        self.orders = _Orders(self._con)

    def updateSummaryFile(self, myHat, myOrder):
        c = self._con.cursor()
        c.execute("""
        SELECT hats.topping , suppliers.name , orders.location FROM hats JOIN orders JOIN suppliers
         ON hats.id=orders.hat AND hats.supplier = suppliers.id WHERE orders.id=? AND hats.id = ? 
        """, [myOrder, myHat])
        return c.fetchone()

    def close(self):
        self._con.commit()
        self._con.close()

    def create_tables(self):
        self._con.executescript("""
        CREATE TABLE hats (
            id          INT         PRIMARY KEY,
            topping     TEXT        NOT NULL,
            supplier    INT         NOT NULL,
            quantity    INT         NOT NULL,
            
            FOREIGN KEY(supplier)   REFERENCES suppliers(id)
        );

        CREATE TABLE suppliers (
            id       INT     PRIMARY KEY,
            name     TEXT    NOT NULL
        );

        CREATE TABLE orders (
            id        INT     PRIMARY KEY,
            location  TEXT    NOT NULL,
            hat       INT     NOT NULL,

            FOREIGN KEY(hat)  REFERENCES hats(id)
        );
    """)


repo = _Repository()
atexit.register(repo.close)
repo.create_tables()

with open(sys.argv[1], 'r') as f_config:
    sizes = f_config.readline()
    l = sizes.split(',')
    num1 = int(l[0])
    num2 = int(l[1])
    for hat in range(num1):
        current = f_config.readline()
        myHat = current.split(',')
        repo.hats.insert(Hat(int(myHat[0]), myHat[1], int(myHat[2]), int(myHat[3])))

    for publisher in range(num2):
        current = f_config.readline()
        mySupp = current.split(',')
        if mySupp[1][len(mySupp[1]) - 1] == '\n':
            mySupp[1] = mySupp[1][:-1]
        repo.suppliers.insert(Supplier(int(mySupp[0]), mySupp[1]))

f_output = open(sys.argv[3], 'w')

with open(sys.argv[2], 'r') as f_orders:
    counter = 1
    for line in f_orders:
        curOrder = line.split(',')
        type = curOrder[1]
        if type[len(type) - 1] == '\n':
            type = type[:-1]
        hatId = repo.hats.find(type)
        repo.orders.insert(Order(counter, curOrder[0], hatId))
        tup = repo.updateSummaryFile(hatId, counter)
        tuplist = [str(tup[0]), str(tup[1]), str(tup[2])]
        f_output.write(','.join(tuplist) + '\n')
        repo.hats.updateInBase(hatId)
        counter += 1
