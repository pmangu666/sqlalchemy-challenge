# Import the dependencies.
from flask import Flask, jsonify
from sqlalchemy import create_engine, func 
from sqlalchemy.ext.automap import automap_base 
from sqlalchemy.orm import Session 
import datetime as dt 
import pandas as pd
import os # impporting the os module for file path operations

#################################################
# Database Setup
#################################################
#define the path to the SQLite db
db_path = os.path.join(os.path.dirname(__file__), 'Resources', 'hawaii.sqlite') 
# create an engine to connect to the SQLite db
engine = create_engine(f"sqlite:///{db_path}")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
#define the root route that displays all available api routes
@app.route("/")
def welcome():
    return(
        f"Welcome to the Climate App API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )

# define route for precipitation data
@app.route("/api/v1.0/precipitation")
def precipitation():
        session = Session(engine) #create session to connect to db
        most_recent_date = session.query(func.max(Measurement.date)).scalar() # calculate the date one year ago from last date in db
        one_year_ago = (pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)).strftime('%Y-%m-%d') 
        results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago).all() # query db for precipitation data from last year
        session.close() #close sesstion
        precipitation_data = {date: prcp for date, prcp in results} # convert query results to a dictionary with date as key and precipitation as the value
        return jsonify(precipitation_data) #return json representation of precipitation dictionary

#define route for station data
@app.route("/api/v1.0/stations")
def stations(): 
        session = Session(engine) # create session to connect to db
        results = session.query(Station.station).all() #query db for all station data
        session.close() #close session
        stations = [station[0] for station in results] #convert query results to list
        return jsonify(stations)   #return json representation of station list

#define route for tobs
@app.route("/api/v1.0/tobs")
def tobs():
        session = Session(engine) #create session to connect to db
        most_recent_date = session.query(func.max(Measurement.date)).scalar() #calculate the date one year ago from last date in db
        one_year_ago = (pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')
        most_active_station = 'USC00519281' #query db for tobs from most active station in last year
        results = session.query(Measurement.date, Measurement.tobs).filter(
            Measurement.station == most_active_station,
            Measurement.date >= one_year_ago
        ).all()

        session.close() #close station
        
        tobs_data = [{'date': date, 'tobs': tobs} for date, tobs in results] #convert query results to list of dictionaries

        return jsonify(tobs_data) #return json representation of tobs list


#define route for temperature statistics
@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def stats(start, end=None):
    session = Session(engine) #create session to connect to db
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)] #define the functions for minimum, average, and maximum temps
    
    #perform the query depending on whether an end date is provided
    if end:
        results = session.query(*sel).filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    else:
        results = session.query(*sel).filter(Measurement.date >= start).all()

    session.close() #close the session

    temp_stats = list(results[0]) #convert the query results to a list

    return jsonify(temp_stats) #return the json representation of temperature statistics

#run flask app
if __name__ == '__main__':
            app.run(debug=True)
