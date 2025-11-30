import pandas as pd
from neo4j import GraphDatabase
import os
import sys

# 1. Configuration Loader
def load_config(file_path='config.txt'):
    config = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    config[key] = value
        return config
    except FileNotFoundError:
        print(f"Error: {file_path} not found.")
        return None

# 2. Graph Builder Class
class AirlineGraphBuilder:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def clear_database(self):
        print("Clearing existing database...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Database cleared.")

    def create_constraints(self):
        print("Creating constraints...")
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT passenger_id IF NOT EXISTS FOR (p:Passenger) REQUIRE p.record_locator IS UNIQUE")
            session.run("CREATE CONSTRAINT journey_id IF NOT EXISTS FOR (j:Journey) REQUIRE j.feedback_ID IS UNIQUE")
            session.run("CREATE CONSTRAINT airport_id IF NOT EXISTS FOR (a:Airport) REQUIRE a.station_code IS UNIQUE")
            # Create index for Flight (composite key logic is handled in MERGE)
            session.run("CREATE INDEX flight_idx IF NOT EXISTS FOR (f:Flight) ON (f.flight_number, f.fleet_type_description)")

    def load_csv_data(self, csv_path):
        if not os.path.exists(csv_path):
            print(f"Error: CSV file not found at {csv_path}")
            return

        print(f"Reading {csv_path}...")
        df = pd.read_csv(csv_path)

        # ---------------------------------------------------------
        # DATA VALIDATION (Based on Inspection)
        # ---------------------------------------------------------
        # The inspection confirmed these specific headers exist:
        required_cols = [
            'origin_station_code', 'destination_station_code', 'flight_number', 
            'fleet_type_description', 'record_locator', 'loyalty_program_level', 
            'generation', 'food_satisfaction_score', 'arrival_delay_minutes', 
            'actual_flown_miles', 'number_of_legs', 'passenger_class', 'feedback_ID'
        ]
        
        # Verify columns exist
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            print(f"CRITICAL ERROR: Missing columns: {missing}")
            print(f"Available columns: {df.columns.tolist()}")
            return

        # Fill NaNs
        df.fillna(0, inplace=True)
        # ---------------------------------------------------------

        total_rows = len(df)
        print(f"Found {total_rows} records. Starting import...")

        import_query = """
        UNWIND $batch AS row
        
        MERGE (origin:Airport {station_code: row.origin_station_code})
        MERGE (dest:Airport {station_code: row.destination_station_code})
        
        MERGE (f:Flight {
            flight_number: row.flight_number, 
            fleet_type_description: row.fleet_type_description
        })
        
        MERGE (f)-[:DEPARTS_FROM]->(origin)
        MERGE (f)-[:ARRIVES_AT]->(dest)
        
        MERGE (p:Passenger {record_locator: row.record_locator})
        SET p.loyalty_program_level = row.loyalty_program_level,
            p.generation = row.generation
            
        MERGE (j:Journey {feedback_ID: row.feedback_ID})
        SET j.food_satisfaction_score = toInteger(row.food_satisfaction_score),
            j.arrival_delay_minutes = toInteger(row.arrival_delay_minutes),
            j.actual_flown_miles = toInteger(row.actual_flown_miles),
            j.number_of_legs = toInteger(row.number_of_legs),
            j.passenger_class = row.passenger_class
            
        MERGE (p)-[:TOOK]->(j)
        MERGE (j)-[:ON]->(f)
        """

        batch_size = 500
        batch = []
        
        with self.driver.session() as session:
            for index, row in df.iterrows():
                # Direct mapping using the column names found in inspection
                row_data = {
                    'origin_station_code': str(row['origin_station_code']),
                    'destination_station_code': str(row['destination_station_code']),
                    'flight_number': str(row['flight_number']),
                    'fleet_type_description': str(row['fleet_type_description']),
                    'record_locator': str(row['record_locator']),
                    'loyalty_program_level': str(row['loyalty_program_level']),
                    'generation': str(row['generation']),
                    'feedback_ID': str(row['feedback_ID']), # Using existing ID from CSV
                    'food_satisfaction_score': row['food_satisfaction_score'],
                    'arrival_delay_minutes': row['arrival_delay_minutes'],
                    'actual_flown_miles': row['actual_flown_miles'],
                    'number_of_legs': row['number_of_legs'],
                    'passenger_class': str(row['passenger_class']) # Correct column name
                }
                
                batch.append(row_data)
                
                if len(batch) >= batch_size:
                    session.run(import_query, batch=batch)
                    batch = []
                    print(f"Processed {index + 1}/{total_rows} rows...")
            
            if batch:
                session.run(import_query, batch=batch)
                print(f"Processed {total_rows}/{total_rows} rows.")

if __name__ == "__main__":
    config = load_config()
    
    if config:
        try:
            builder = AirlineGraphBuilder(config['URI'], config['USERNAME'], config['PASSWORD'])
            builder.clear_database()
            builder.create_constraints()
            builder.load_csv_data('Airline_surveys_sample.csv')
            builder.close()
            print("Knowledge Graph created successfully!")
            
        except Exception as e:
            print(f"An error occurred: {e}")
