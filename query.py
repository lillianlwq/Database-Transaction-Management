from flask import Flask, g, request, jsonify
import pyodbc
from connect_db import connect_db
import sys
import time, datetime


app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'azure_db'):
        g.azure_db = connect_db()
        g.azure_db.autocommit = True
        g.azure_db.set_attr(pyodbc.SQL_ATTR_TXN_ISOLATION, pyodbc.SQL_TXN_SERIALIZABLE)
    return g.azure_db

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'azure_db'):
        g.azure_db.close()



@app.route('/login')
def login():
    username = request.args.get('username', "")
    password = request.args.get('password', "")
    cid = -1
    print (username, password)
    conn = get_db()
    #print (conn)
    cursor = conn.execute("SELECT * FROM Customer WHERE username = ? AND password = ?", (username, password))
    records = cursor.fetchall()
    #print records
    if len(records) != 0:
        cid = records[0][0]
    response = {'cid': cid}
    return jsonify(response)




@app.route('/getRenterID')
def getRenterID():
    """
       This HTTP method takes mid as input, and
       returns cid which represents the customer who is renting the movie.
       If this movie is not being rented by anyone, return cid = -1
    """
    mid = int(request.args.get('mid', -1))
    # WRITE YOUR CODE HERE
    
    cid = -1
    conn = get_db()
    cursor = conn.execute("SELECT * FROM Rental WHERE mid = ? AND status = 'open'", (mid))
    records = cursor.fetchall()
    if (len(records)) != 0:
        cid = records[0][0]
   
    response = {'cid': cid}
    return response



@app.route('/getRemainingRentals')
def getRemainingRentals():
    """
        This HTTP method takes cid as input, and returns n which represents
        how many more movies that cid can rent.

        n = 0 means the customer has reached its maximum number of rentals.
    """
    conn = get_db()
    cid = int(request.args.get('cid', -1))


    # Tell ODBC that you are starting a multi-statement transaction
    conn.autocommit = False

    # WRITE YOUR CODE HERE
    n = 0
    pid = -1 
    
    # Get user's plan record
    getPID = "SELECT pid FROM Customer WHERE cid = ?"
    cursor = conn.execute(getPID, (cid))
    records = cursor.fetchall()
    if (len(records)) != 0:
        pid = records[0][0]
        
    # Get max movies in the user's plan    
    if (pid != -1):
        getMaxMovies = "SELECT max_movies FROM RentalPlan WHERE pid = ?"
        cursor = conn.execute(getMaxMovies, (pid))
        records = cursor.fetchall()
        if (len(records)) != 0:
            maxMovies = records[0][0]
        # Get number of movies currently rented  
        getCurRentMovies = "SELECT count(status) FROM Rental WHERE cid = ? and status = 'open'"
        cursor = conn.execute(getCurRentMovies, (cid))
        records = cursor.fetchall()
        if (len(records)) != 0:
            curRentMovies = records[0][0]
    conn.autocommit = True

    if (pid != -1):
        n = maxMovies - curRentMovies # calculate remaining rentals 
    else:
        n = 0
        
    response = {"remain": n}
    return jsonify(response)





def currentTime():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


@app.route('/rent')
def rent():
    """
        This HTTP method takes cid and mid as input, and returns either "success" or "fail".

        It returns "fail" if C1, C2, or both are violated:
            C1. at any time a movie can be rented to at most one customer.
            C2. at any time a customer can have at most as many movies rented as his/her plan allows.
        Otherwise, it returns "success" and also updates the database accordingly.
    """
    cid = int(request.args.get('cid', -1))
    mid = int(request.args.get('mid', -1))
    remaining = 0
    conn = get_db()
    pid = -1
     # Tell ODBC that you are starting a multi-statement transaction
    conn.autocommit = False

    # WRITE YOUR CODE HERE
    isValidMovie = "SELECT * FROM Movie WHERE mid = ?"
    cursor = conn.execute(isValidMovie, (mid))
    records = cursor.fetchall()
    if (len(records)) != 0: # this is a valid movie, then check its renting status
        getRentStatus = "SELECT status FROM Rental WHERE mid = ?"
        cursor = conn.execute(getRentStatus, (mid))
        statusRecords = cursor.fetchall()

        if (len(statusRecords) == 0):  # No renting records at all
            isRented = False # can be rented
        elif (len(statusRecords) == 1 and "open" in statusRecords[0]): # rented but not returned yet
            isRented = True # cannot be rented
        elif (len(statusRecords) == 1 and "closed" in statusRecords[0]): # returned
            isRented = False
        elif (len(statusRecords) > 1): # check up-to-date status if there are more than 1 corresponding records
            getLatestStatus = "SELECT status, date_and_time FROM Rental WHERE mid = ? ORDER BY date_and_time DESC"
            cursor = conn.execute(getRentStatus, (mid))
            latestStatus= cursor.fetchall()
            if (len(latestStatus)) != 0:
                print(latestStatus, file = sys.stderr) # get latest status
                if latestStatus[0][0] == "closed": 
                    isRented = False # can be rented
                else:
                    isRented = True # cannot be rented
        
        if (isRented == False): # can be rented, then check user's remaining credit
            getPID = "SELECT pid FROM Customer WHERE cid = ?"
            cursor = conn.execute(getPID, (cid))
            records = cursor.fetchall()
            if (len(records)) != 0: # valid cid
                pid = records[0][0]
            if (pid != -1): # valid pid
                getMaxMovies = "SELECT max_movies FROM RentalPlan WHERE pid = ?"
                cursor = conn.execute(getMaxMovies, (pid))
                records = cursor.fetchall()
                if (len(records)) != 0:
                    maxMovies = records[0][0] # max numbers of movies in the plan
                getCurRentMovies = "SELECT count(status) FROM Rental WHERE cid = ? and status = 'open'"
                cursor = conn.execute(getCurRentMovies, (cid))
                records = cursor.fetchall()
                if (len(records)) != 0:
                    curRentMovies = records[0][0] # current number of renting movies
            if (pid != -1):
                remaining = maxMovies - curRentMovies # get remaining credit
            else:
                remaining = 0
            if (remaining == 0): # no credit -> user cannot rent any movie
                conn.autocommit = True
                response = {"rent": "fail"}
                return jsonify(response)
            else: # has credit -> user can rent the movie
                addRentRecord = "INSERT INTO Rental VALUES(?,?,?,?)"  # update Rental table by inserting a new record
                dateTime = currentTime()
                fields = [cid, mid, dateTime, "open"]
                cursor.execute(addRentRecord, fields)
                conn.autocommit = True
                response = {"rent": "success"}
                return jsonify(response) 
        elif(isRented == True): # the movie have not been returned yet
            conn.autocommit = True
            response = {"rent": "fail"}
            return jsonify(response)
    else: # invalid movie, cannot be rented    
        conn.autocommit = True
        response = {"rent": "fail"}
        return jsonify(response)

