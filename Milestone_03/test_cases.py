import time
from pprint import pprint
from query_generation import generate_cypher
from intent_and_NER import classify_intent_llm, extract_entities, map_airports_to_codes, airport_code_to_city, process_user_query


def test_intent_classifier():
    test_queries = [
        # Flights / Airports
        "List all flights arriving at IAX today",
        "Which flights use the B737-MAX8 fleet?",
        "How many flights depart from DEX and arrive at LAX?",

        # Journey / Performance
        "What was the average arrival delay for flight 924?",
        "Show me the journeys with more than 2 legs",
        "Did flight 659 arrive on time last week?",

        # Passenger / Loyalty
        "What is the generation of passenger M4XXTP?",
        "How many premier gold passengers are there?",
        "Give me loyalty program statistics for Gen X passengers",
        "List passengers who flew on flight 2614",

        # Satisfaction
        "What was the food satisfaction score for flight 5372?",
        "Which flights had the lowest food ratings?",

        # Aggregation / Comparison
        "Top 5 flights with the most miles flown",
        "Compare the arrival delays of LAX and DEX",
        "Average number of legs per passenger class",

        # Unsupported / Edge cases
        "How much did the ticket cost for flight 924?",
        "Is there WiFi on the flight to IAX?",
        "What will the weather be at LAX tomorrow?",
        "Can I get a refund for my flight?",

        # Ambiguous / tricky
        "Tell me about passengers flying on economy class",
        "Which generation had the highest satisfaction score?"
    ]

    for q in test_queries:
        result = classify_intent_llm(q)
        print("-" * 60)
        print(f"Query: {q}")
        print(f"Intent: {result['intent']}")
        print(f"Confidence: {result['confidence']:.2f}")

def test_entity_extraction():
    test_queries = [
        # Flight + Airport
        "Which flights did passenger BTXXE0 take from LAX to IAX in Economy class?",
        "What was the food satisfaction score and arrival delay for flight 924?",

        # Passenger / Journey
        "What flights did passenger M4XXTP take in premier gold class?",

        # Mixed / multiple entities
        "Can you provide details for passenger NZXX1X, including the aircraft type and miles flown?"
    ]

    for q in test_queries:
        print("=" * 60)
        print(f"User question: {q}")
        entities = extract_entities(q)
        pprint(entities)
        print("\n")
        time.sleep(30)

def test_map_airports_to_codes():
    extracted_entities = {
        "passengers": [
            {"record_locator": "BTXXE0", "loyalty_program_level": "non-elite", "generation": "Millennial"}
        ],
        "journeys": [
            {"feedback_ID": "F_1", "food_satisfaction_score": 3, "arrival_delay_minutes": -13,
             "actual_flown_miles": 1379, "number_of_legs": 3, "passenger_class": "Economy"}
        ],
        "flights": [
            {"flight_number": "2411", "fleet_type_description": "B777-200"}
        ],
        "airports": [
            {"station_code": "Los Angeles"},
            {"station_code": "IAX"}# city name instead of IATA code
        ],
        "relations": {
            "took": [
                {"passenger": "BTXXE0", "journey": "F_1"}
            ],
            "on": [
                {"journey": "F_1", "flight": "2411"}
            ],
            "departs_from": [
                {"flight": "2411", "airport": "Los Angeles"}
            ],
            "arrives_at": [
                {"flight": "2411", "airport": "IAX"}
            ]
        }
    }

    print("Before mapping airports:")
    pprint(extracted_entities)

    # Map airports to IATA codes
    mapped_entities = map_airports_to_codes(extracted_entities, airport_code_to_city)

    print("\nAfter mapping airports:")
    pprint(mapped_entities)

if __name__ == "__main__":
    intent, entities = process_user_query("What was the food satisfaction score for flight 5372?")
    cypher = generate_cypher(intent, entities)
    print("\nCYPHER:")
    print(cypher)

