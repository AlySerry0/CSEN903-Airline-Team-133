import json
from neo4j import GraphDatabase
import math

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

# 2. Expected Results (Hardcoded)
EXPECTED_RESULTS = {
    1: [
        {"origin": "LAX", "destination": "IAX", "flight_count": 21},
        {"origin": "LAX", "destination": "EWX", "flight_count": 17},
        {"origin": "IAX", "destination": "LAX", "flight_count": 17},
        {"origin": "SAX", "destination": "IAX", "flight_count": 15},
        {"origin": "IAX", "destination": "EWX", "flight_count": 14}
    ],
    2: [
        {"flight_id": 42, "feedback_count": 14},
        {"flight_id": 19, "feedback_count": 13},
        {"flight_id": 86, "feedback_count": 12},
        {"flight_id": 27, "feedback_count": 12},
        {"flight_id": 966, "feedback_count": 12},
        {"flight_id": 57, "feedback_count": 11},
        {"flight_id": 1686, "feedback_count": 11},
        {"flight_id": 219, "feedback_count": 11},
        {"flight_id": 819, "feedback_count": 9},
        {"flight_id": 991, "feedback_count": 9}
    ],
    3: [
        {"generation": "Boomer", "multi_leg_count": 498, "avg_score": 2.7911646586345387},
        {"generation": "Gen X", "multi_leg_count": 285, "avg_score": 2.999999999999999},
        {"generation": "Millennial", "multi_leg_count": 130, "avg_score": 2.738461538461538},
        {"generation": "Silent", "multi_leg_count": 48, "avg_score": 2.6874999999999996},
        {"generation": "Gen Z", "multi_leg_count": 18, "avg_score": 3.2777777777777777}
    ],
    4: [
        {"flight_id": 2442, "avg_arrival_delay": -99.0},
        {"flight_id": 274, "avg_arrival_delay": -60.0},
        {"flight_id": 425, "avg_arrival_delay": -59.0},
        {"flight_id": 982, "avg_arrival_delay": -46.0},
        {"flight_id": 120, "avg_arrival_delay": -45.0},
        {"flight_id": 1237, "avg_arrival_delay": -44.0},
        {"flight_id": 894, "avg_arrival_delay": -42.5},
        {"flight_id": 3546, "avg_arrival_delay": -42.0},
        {"flight_id": 942, "avg_arrival_delay": -41.333333333333336},
        {"flight_id": 828, "avg_arrival_delay": -41.0}
    ],
    5: [
        {"loyalty_level": "global services", "avg_actual_flown_miles": 2648.083333333333},
        {"loyalty_level": "premier gold", "avg_actual_flown_miles": 2461.018518518519},
        {"loyalty_level": "premier platinum", "avg_actual_flown_miles": 2420.5714285714294},
        {"loyalty_level": "non-elite", "avg_actual_flown_miles": 2254.072},
        {"loyalty_level": "premier silver", "avg_actual_flown_miles": 2068.673529411763},
        {"loyalty_level": "NBK", "avg_actual_flown_miles": 1989.0},
        {"loyalty_level": "premier 1k", "avg_actual_flown_miles": 1897.6666666666672}
    ]
}

# 3. Cypher Queries
QUERIES = {
    1: """
        MATCH (f:Flight)-[:DEPARTS_FROM]->(origin:Airport)
        MATCH (f)-[:ARRIVES_AT]->(dest:Airport)
        RETURN origin.station_code AS origin, 
               dest.station_code AS destination, 
               count(f) AS flight_count
        ORDER BY flight_count DESC, origin DESC
        LIMIT 5
    """,
    2: """
        MATCH (j:Journey)-[:ON]->(f:Flight)
        RETURN toInteger(f.flight_number) AS flight_id, 
               count(j) AS feedback_count
        ORDER BY feedback_count DESC
        LIMIT 10
    """,
    3: """
        MATCH (p:Passenger)-[:TOOK]->(j:Journey)
        WHERE j.number_of_legs > 1
        RETURN p.generation AS generation, 
               count(j) AS multi_leg_count, 
               avg(j.food_satisfaction_score) AS avg_score
        ORDER BY multi_leg_count DESC
    """,
    4: """
        MATCH (j:Journey)-[:ON]->(f:Flight)
        RETURN toInteger(f.flight_number) AS flight_id, 
               avg(j.arrival_delay_minutes) AS avg_arrival_delay
        ORDER BY avg_arrival_delay ASC
        LIMIT 10
    """,
    5: """
        MATCH (p:Passenger)-[:TOOK]->(j:Journey)
        RETURN p.loyalty_program_level AS loyalty_level, 
               avg(j.actual_flown_miles) AS avg_actual_flown_miles
        ORDER BY avg_actual_flown_miles DESC
    """
}

def compare_results(query_num, actual, expected):
    print(f"\n--- Validating Query {query_num} ---")
    
    if len(actual) != len(expected):
        print(f"FAIL: Length mismatch. Expected {len(expected)}, got {len(actual)}")
        return False

    all_match = True
    for i, (act_row, exp_row) in enumerate(zip(actual, expected)):
        row_match = True
        for key, exp_val in exp_row.items():
            act_val = act_row.get(key)
            
            # Handle floating point comparisons
            if isinstance(exp_val, float) and isinstance(act_val, (int, float)):
                if not math.isclose(act_val, exp_val, rel_tol=1e-9):
                    row_match = False
            # Handle standard equality
            elif act_val != exp_val:
                row_match = False
            
            if not row_match:
                print(f"Mismatch at Row {i}, Key '{key}': Expected {exp_val}, Got {act_val}")
                all_match = False
                break
        
    if all_match:
        print("PASS")
    else:
        print("FAIL")

def run_validation():
    config = load_config()
    if not config:
        return

    driver = GraphDatabase.driver(config['URI'], auth=(config['USERNAME'], config['PASSWORD']))

    with driver.session() as session:
        for q_num in range(1, 6):
            result = session.run(QUERIES[q_num])
            actual_data = [dict(record) for record in result]
            compare_results(q_num, actual_data, EXPECTED_RESULTS[q_num])

    driver.close()

if __name__ == "__main__":
    run_validation()