import pyodbc
from connect_db import connect_db


def loadRentalPlan(filename, conn):
    """
        Input:
            $filename: "RentalPlan.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "RentalPlan" in the "VideoStore" database on Azure
            2. Read data from "RentalPlan.txt" and insert them into "RentalPlan"
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    cursor = conn.cursor()
    cursor.execute(
        '''
        CREATE TABLE RentalPlan
        (
        pid INT PRIMARY KEY,
        pname VARCHAR(50),
        monthly_fee FLOAT,
        max_movies INT      
        )
        '''   
    )
    with open(filename) as f:
        rowLists = f.read().splitlines() 
    fields = [line.split("|") for line in rowLists]
    insertQuary = "INSERT INTO RentalPlan VALUES(?,?,?,?)"
    cursor.executemany(insertQuary,fields)
                    


def loadCustomer(filename, conn):
    """
        Input:
            $filename: "Customer.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Customer" in the "VideoStore" database on Azure
            2. Read data from "Customer.txt" and insert them into "Customer".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    cursor = conn.cursor()
    cursor.execute(
        '''
        CREATE TABLE Customer(
        cid INT PRIMARY KEY,
        pid INT, 
        username VARCHAR(50),
        password VARCHAR(50)
        FOREIGN KEY (pid) REFERENCES RentalPlan(pid)
        ON DELETE CASCADE
        )
        '''
    )
    with open(filename) as f:
        rowLists = f.read().splitlines() 
    fields = [line.split("|") for line in rowLists]
    insertQuary = "INSERT INTO Customer VALUES(?,?,?,?)"
    cursor.executemany(insertQuary,fields)



def loadMovie(filename, conn):
    """
        Input:
            $filename: "Movie.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Movie" in the "VideoStore" database on Azure
            2. Read data from "Movie.txt" and insert them into "Movie".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    cursor = conn.cursor()
    cursor.execute(
        '''
        CREATE TABLE Movie
        (
        mid INT PRIMARY KEY,
        mname VARCHAR(50),
        year INT      
        )
        '''   
    )
    with open(filename) as f:
        rowLists = f.read().splitlines() 
    fields = [line.split("|") for line in rowLists]
    insertQuary = "INSERT INTO Movie VALUES(?,?,?)"
    cursor.executemany(insertQuary,fields)

def loadRental(filename, conn):
    """
        Input:
            $filename: "Rental.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Rental" in the VideoStore database on Azure
            2. Read data from "Rental.txt" and insert them into "Rental".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    cursor = conn.cursor()
    cursor.execute(
        '''
        CREATE TABLE Rental
        (cid INT FOREIGN KEY REFERENCES Customer(cid),
        mid INT,
        date_and_time DATETIME,
        status VARCHAR(6),
        FOREIGN KEY (mid) REFERENCES Movie(mid)
        ON DELETE CASCADE
        )
        '''   
    )
    with open(filename) as f:
        rowLists = f.read().splitlines() 
    fields = [line.split("|") for line in rowLists]
    insertQuary = "INSERT INTO Rental VALUES(?,?,?,?)"
    cursor.executemany(insertQuary,fields)



def dropTables(conn):
    conn.execute("DROP TABLE IF EXISTS Rental")
    conn.execute("DROP TABLE IF EXISTS Customer")
    conn.execute("DROP TABLE IF EXISTS RentalPlan")
    conn.execute("DROP TABLE IF EXISTS Movie")



if __name__ == "__main__":
    conn = connect_db()

    dropTables(conn)

    loadRentalPlan("RentalPlan.txt", conn)
    loadCustomer("Customer.txt", conn)
    loadMovie("Movie.txt", conn)
    loadRental("Rental.txt", conn)


    conn.commit()
    conn.close()






