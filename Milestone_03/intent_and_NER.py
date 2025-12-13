import pandas as pd
from openai import OpenAI
from prompts import INTENT_SYSTEM_PROMPT,ENTITY_SYSTEM_PROMPT
import json
import os

endpoint = "https://models.github.ai/inference"
client = OpenAI(base_url=endpoint, api_key="token")


#Create a map of city names -> airport codes
airline_df = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)),"Airline_surveys_sample.csv"))
airport_map = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)),"airport-codes.csv"))

airport_codes = pd.concat([airline_df["origin_station_code"], airline_df["destination_station_code"]]).dropna()

unique_airport_codes = airport_codes.str.upper().str.strip().unique().tolist()

airport_map["iata_code"] = airport_map["iata_code"].str.upper().str.strip()

filtered = airport_map[airport_map["iata_code"].isin(unique_airport_codes)]

airport_code_to_city = dict(
    zip(filtered["iata_code"], filtered["municipality"])
)

#Classify Intent from user prompt
def classify_intent_llm(user_text: str):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": INTENT_SYSTEM_PROMPT},
            {"role": "user", "content": f'User question:\n"{user_text}"'}
        ],
        temperature=0
    )
    raw = response.choices[0].message.content.strip()
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        return {"intent": "unsupported_query", "confidence": 0.0}

    if result.get("confidence", 0) < 0.5:
        return {"intent": "unsupported_query", "confidence": result.get("confidence", 0)}

    return result

#Extract Entities from user prompt
def extract_entities(user_text: str) -> dict:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": ENTITY_SYSTEM_PROMPT},
            {"role": "user", "content": f'User question:\n"{user_text}"'}
        ],
        temperature=0
    )
    content = response.choices[0].message.content
    try:
        entities = json.loads(content)
    except json.JSONDecodeError:
        entities = {"raw_output": content}

    return entities

#Map city names back to codes
def map_airports_to_codes(extracted_entities, airport_code_to_city):
    city_to_code = {str(city).upper(): code for code, city in airport_code_to_city.items() if isinstance(city, str)}

    for airport in extracted_entities.get("airports", []):
        code_or_city = str(airport.get("station_code", "")).upper()
        if code_or_city in airport_code_to_city:
            continue
        airport["station_code"] = city_to_code.get(code_or_city, None)

    for relation_type in ["departs_from", "arrives_at"]:
        for rel in extracted_entities.get("relations", {}).get(relation_type, []):
            airport_val = str(rel.get("airport", "")).upper()
            if airport_val in airport_code_to_city:
                rel["airport"] = airport_val
            else:
                rel["airport"] = city_to_code.get(airport_val, None)

    return extracted_entities

#Validate entities by checking whether they exist in dataset
def validate_entities(entity_dict, df):
    valid_values = {
        col: {str(v).strip().upper() for v in df[col].dropna().unique()}
        for col in df.columns
    }

    def validate_single_dict(obj):
        removed_fields = {}
        for field, value in list(obj.items()):
            if value == "" or value is None:
                continue

            if field not in valid_values:
                continue

            normalized_value = str(value).strip().upper()

            if normalized_value not in valid_values[field]:
                print(
                    f"This value '{value}' does not exist in {field} — "
                    f"searching all {field} instead."
                )
                removed_fields[field] = value
                obj[field] = ""
        return obj, removed_fields

    removed_global = {
        "passengers": [],
        "journeys": [],
        "flights": [],
        "airports": []
    }

    if "passengers" in entity_dict:
        new_list = []
        for p in entity_dict["passengers"]:
            updated, removed = validate_single_dict(p)
            new_list.append(updated)
            removed_global["passengers"].append(removed)
        entity_dict["passengers"] = new_list

    if "journeys" in entity_dict:
        new_list = []
        for j in entity_dict["journeys"]:
            updated, removed = validate_single_dict(j)
            new_list.append(updated)
            removed_global["journeys"].append(removed)
        entity_dict["journeys"] = new_list

    if "flights" in entity_dict:
        new_list = []
        for f in entity_dict["flights"]:
            updated, removed = validate_single_dict(f)
            new_list.append(updated)
            removed_global["flights"].append(removed)
        entity_dict["flights"] = new_list

    if "airports" in entity_dict:
        new_list = []
        for a in entity_dict["airports"]:
            updated, removed = validate_single_dict(a)
            new_list.append(updated)
            removed_global["airports"].append(removed)
        entity_dict["airports"] = new_list

    if "relations" in entity_dict:
        for rel_type, rel_list in entity_dict["relations"].items():
            cleaned_list = []
            for rel in rel_list:
                for key, value in list(rel.items()):
                    if value == "" or value is None:
                        continue

                    normalized = str(value).strip().upper()
                    still_valid = any(
                        normalized in {str(v).strip().upper() for v in item.values()}
                        for section in ["passengers", "journeys", "flights", "airports"]
                        for item in entity_dict.get(section, [])
                    )
                    if not still_valid:
                        print(
                            f"Relation reference '{value}' in '{rel_type}' no longer valid — removing it."
                        )
                        rel[key] = ""
                cleaned_list.append(rel)
            entity_dict["relations"][rel_type] = cleaned_list
    return entity_dict

def process_user_query(user_text: str):
    intent_result = classify_intent_llm(user_text)
    intent = intent_result.get("intent", "unsupported_query")
    entities = extract_entities(user_text)
    entities = map_airports_to_codes(entities, airport_code_to_city)
    validated_entities = validate_entities(entities, airline_df)
    return intent, validated_entities