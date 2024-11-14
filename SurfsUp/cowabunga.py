from flask import Flask, jsonify
import datetime as dt
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Database Setup
engine = create_engine("sqlite:///hawaii.sqlite")
Base = declarative_base()

class Measurement(Base):
    __tablename__ = 'measurements'
    id = Column(Integer, primary_key=True)
    station = Column(String)
    date = Column(Date)
    prcp = Column(Float)
    tobs = Column(Float)

class Station(Base):
    __tablename__ = 'stations'
    id = Column(Integer, primary_key=True)
    station = Column(String)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)

Session = sessionmaker(bind=engine)
app = Flask(__name__)

@app.route("/")
def welcome():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation - Get last 12 months of precipitation data.<br/>"
        f"/api/v1.0/stations - Get list of weather stations.<br/>"
        f"/api/v1.0/tobs - Get temperature observations of the most active station for the last year.<br/>"
        f"/api/v1.0/<start> - Get min, avg, and max temperatures from the start date (YYYY-MM-DD).<br/>"
        f"Example: /api/v1.0/2016-08-23<br/>"
        f"/api/v1.0/<start>/<end> - Get min, avg, and max temperatures for the date range (YYYY-MM-DD).<br/>"
        f"Example: /api/v1.0/2016-08-23/2017-08-23<br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session()
    try:
        most_recent_date = session.query(func.max(Measurement.date)).scalar()
        one_year_ago = most_recent_date - dt.timedelta(days=365)
        precipitation_data = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago).all()
        precipitation_dict = {date.isoformat(): prcp for date, prcp in precipitation_data}
        return jsonify(precipitation_dict)
    finally:
        session.close()

@app.route("/api/v1.0/stations")
def stations():
    session = Session()
    try:
        results = session.query(Station.station, Station.name).all()
        stations_list = [{"station": station, "name": name} for station, name in results]
        return jsonify(stations_list)
    finally:
        session.close()

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session()
    try:
        # Identify the most active station
        most_active_station = session.query(Measurement.station).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()[0]
        
        # Confirm that the most active station is USC00519281
        if most_active_station != 'USC00519281':
            return jsonify({"error": "Most active station is not USC00519281"}), 400
        
        # Find the most recent date in the dataset
        most_recent_date = session.query(func.max(Measurement.date)).scalar()
        one_year_ago = most_recent_date - dt.timedelta(days=365)
        
        # Retrieve the temperature observations (TOBS) for the last year
        tobs_results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == most_active_station).filter(Measurement.date >= one_year_ago).all()
        tobs_list = [{"date": date.isoformat(), "tobs": tobs} for date, tobs in tobs_results]
        return jsonify(tobs_list)
    finally:
        session.close()


@app.route("/api/v1.0/<start>")
def start_date(start):
    session = Session()
    try:
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start).all()
        temperature_data = [{"TMIN": tmin, "TAVG": tavg, "TMAX": tmax} for tmin, tavg, tmax in results]
        return jsonify(temperature_data)
    finally:
        session.close()

@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start, end):
    session = Session()
    try:
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start).filter(Measurement.date <= end).all()
        temperature_data = [{"TMIN": tmin, "TAVG": tavg, "TMAX": tmax} for tmin, tavg, tmax in results]
        return jsonify(temperature_data)
    finally:
        session.close()

if __name__ == "__main__":
    app.run(debug=True)
