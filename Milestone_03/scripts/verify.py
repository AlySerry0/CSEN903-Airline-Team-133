import os
import sys

# Ensure we can import from the current directory
# Ensure we can import from the project root
# scripts/verify.py -> scripts -> Milestone_03 (root)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.manager import ConfigManager
from src.data.graph import GraphExecutor
from src.logic.nlp import process_user_query
from src.logic.cypher_gen import generate_cypher

def main():
    print("--- Airline Graph-RAG Verification Script ---")
    
    # 1. Config
    print("\n1. Testing ConfigManager...")
    cm = ConfigManager()
    valid, msg = cm.validate()
    print(f"   Config Status: {'VALID' if valid else 'INVALID'}")
    if not valid:
        print(f"   Message: {msg}")
        print("   -> TIP: Create a .streamlit/secrets.toml file or set env vars.")
    
    # 2. Connection
    if valid:
        print("\n2. Testing Neo4j Connection...")
        try:
            executor = GraphExecutor(
                uri=cm.get("NEO4J_URI"),
                auth=(cm.get("NEO4J_USERNAME"), cm.get("NEO4J_PASSWORD"))
            )
            if executor.verify_connection():
                print("   Neo4j Connection: SUCCESS")
            else:
                print("   Neo4j Connection: FAILED")
                executor.close()
                return
        except Exception as e:
            print(f"   Neo4j Connection Error: {e}")
            return

        # 3. Pipeline Test
        print("\n3. Testing Pipeline Logic (Offline/Mock data for NER if API missing)...")
        # Updated to use airports that exist in the provided Airline_surveys_sample.csv (LAX, IAX)
        query = "Which flights from LAX to IAX have the highest delays?"
        print(f"   Query: {query}")
        
        try:
            # We assume OpenAI might fail if no token, so we wrap this
            intent, entities = process_user_query(query)
            print(f"   Intent: {intent}")
            print(f"   Entities: {entities}")
            
            if intent != "unsupported_query":
                cypher = generate_cypher(intent, entities)
                print(f"   Cypher: {cypher}")
                
                # Execute
                print("   Executing Cypher...")
                results = executor.execute(cypher)
                print(f"   Results Count: {len(results)}")
                print(f"   Sample: {results[:1] if results else 'None'}")
            
        except Exception as e:
            print(f"   Pipeline Execution Error: {e}")
            print("   (This is expected if OpenAI API key is missing or invalid)")

        executor.close()

if __name__ == "__main__":
    main()
