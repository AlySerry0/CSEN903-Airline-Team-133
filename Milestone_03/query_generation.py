def generate_cypher(intent: str, entities: dict) -> str:
    if intent == "satisfaction_insights":
        return cypher_satisfaction_insights(entities)

    if intent == "find_flights":
        return cypher_find_flights(entities)

    if intent == "flight_performance":
        return cypher_flight_performance(entities)

    if intent == "journey_details":
        return cypher_journey_details(entities)

    if intent == "passenger_info":
        return cypher_passenger_info(entities)

    if intent == "loyalty_insights":
        return cypher_loyalty_insights()

    if intent == "generation_insights":
        return cypher_generation_insights()

    if intent == "airport_stats":
        return cypher_airport_stats(entities)

    if intent == "compare_entities":
        return cypher_compare_entities(entities)

    if intent == "aggregation":
        return cypher_aggregation(entities)

    return "// Unsupported intent"

def _where_if(value, clause):
    """Return WHERE clause only if value is present."""
    return f"WHERE {clause}\n" if value not in ("", None) else ""

def _and_if(value, clause):
    """Return AND clause only if value is present."""
    return f"AND {clause}\n" if value not in ("", None) else ""


def cypher_satisfaction_insights(entities: dict) -> str:
    flight = entities["flights"][0]
    journey = entities["journeys"][0]

    q = "MATCH (j:Journey)-[:ON]->(f:Flight)\n"

    q += _where_if(
        flight["flight_number"],
        f"f.flight_number = '{flight['flight_number']}'"
    )

    q += """
RETURN 
  f.flight_number AS flight,
  avg(j.food_satisfaction_score) AS avg_food_rating,
  min(j.food_satisfaction_score) AS min_food_rating,
  max(j.food_satisfaction_score) AS max_food_rating
ORDER BY avg_food_rating ASC
"""
    return q

def cypher_find_flights(entities: dict) -> str:
    flight = entities["flights"][0]
    airport_arr = entities["relations"]["arrives_at"][0]
    airport_dep = entities["relations"]["departs_from"][0]

    q = "MATCH (f:Flight)\n"

    if airport_dep["airport"]:
        q += "MATCH (f)-[:DEPARTS_FROM]->(d:Airport)\n"
    if airport_arr["airport"]:
        q += "MATCH (f)-[:ARRIVES_AT]->(a:Airport)\n"

    q += "WHERE 1=1\n"

    q += _and_if(
        flight["fleet_type_description"],
        f"f.fleet_type_description = '{flight['fleet_type_description']}'"
    )

    q += _and_if(
        airport_dep["airport"],
        f"d.station_code = '{airport_dep['airport']}'"
    )

    q += _and_if(
        airport_arr["airport"],
        f"a.station_code = '{airport_arr['airport']}'"
    )

    q += "RETURN DISTINCT f.flight_number, f.fleet_type_description"

    return q


def cypher_flight_performance(entities: dict) -> str:
    flight = entities["flights"][0]

    q = "MATCH (j:Journey)-[:ON]->(f:Flight)\n"

    q += _where_if(
        flight["flight_number"],
        f"f.flight_number = '{flight['flight_number']}'"
    )

    q += """
RETURN
  f.flight_number AS flight,
  avg(j.arrival_delay_minutes) AS avg_delay,
  max(j.arrival_delay_minutes) AS max_delay,
  min(j.arrival_delay_minutes) AS min_delay
ORDER BY avg_delay DESC
"""
    return q


def cypher_journey_details(entities: dict) -> str:
    journey = entities["journeys"][0]
    flight = entities["flights"][0]

    q = "MATCH (j:Journey)-[:ON]->(f:Flight)\nWHERE 1=1\n"

    q += _and_if(
        flight["flight_number"],
        f"f.flight_number = '{flight['flight_number']}'"
    )

    q += _and_if(
        journey["passenger_class"],
        f"j.passenger_class = '{journey['passenger_class']}'"
    )

    q += """
RETURN
  f.flight_number,
  j.actual_flown_miles,
  j.number_of_legs,
  j.passenger_class
"""
    return q

def cypher_passenger_info(entities: dict) -> str:
    p = entities["passengers"][0]

    q = "MATCH (p:Passenger)\n"

    q += _where_if(
        p["record_locator"],
        f"p.record_locator = '{p['record_locator']}'"
    )

    q += """
RETURN
  p.record_locator,
  p.loyalty_program_level,
  p.generation
"""
    return q

def cypher_loyalty_insights() -> str:
    return """
MATCH (p:Passenger)
RETURN
  p.loyalty_program_level AS loyalty_level,
  count(*) AS count
ORDER BY count DESC
"""

def cypher_generation_insights() -> str:
    return """
MATCH (p:Passenger)
RETURN
  p.generation AS generation,
  count(*) AS count
ORDER BY count DESC
"""

def cypher_airport_stats(entities: dict) -> str:
    airport = entities["airports"][0]

    q = "MATCH (f:Flight)-[:ARRIVES_AT]->(a:Airport)\n"

    q += _where_if(
        airport["station_code"],
        f"a.station_code = '{airport['station_code']}'"
    )

    q += """
RETURN
  a.station_code,
  count(f) AS arriving_flights
ORDER BY arriving_flights DESC
"""
    return q

def cypher_compare_entities(entities: dict) -> str:
    return """
MATCH (j:Journey)-[:ON]->(f:Flight)
RETURN
  f.flight_number,
  avg(j.arrival_delay_minutes) AS avg_delay
ORDER BY avg_delay DESC
"""

def cypher_aggregation(entities: dict) -> str:
    return """
MATCH (j:Journey)-[:ON]->(f:Flight)
RETURN
  f.flight_number,
  avg(j.food_satisfaction_score) AS avg_food
ORDER BY avg_food DESC
LIMIT 5
"""


