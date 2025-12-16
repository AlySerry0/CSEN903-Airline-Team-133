import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.manager import ConfigManager
from src.data.vector import JourneyEmbeddingRetriever, MODEL_CONFIGS

def main():
    print("--- Verifying Embedding Setup Logic ---")
    
    # 1. Config
    cm = ConfigManager()
    valid, msg = cm.validate()
    if not valid:
        print(f"Config Invalid: {msg}")
        return

    # 2. Initialize Retriever with a specific model
    test_model = list(MODEL_CONFIGS.keys())[0] # Pick the first one
    print(f"Testing with model: {test_model}")
    
    retriever = JourneyEmbeddingRetriever(
        uri=cm.get("NEO4J_URI"),
        user=cm.get("NEO4J_USERNAME"),
        password=cm.get("NEO4J_PASSWORD"),
        model_name=test_model
    )

    # 3. Check Active Model (Should be whatever was last used or None)
    active_model = retriever.get_active_model()
    print(f"Current Active Model in DB: {active_model}")

    # 4. Simulate a Setup Call (We won't force rebuild to save time, unless active is None)
    if active_model != test_model:
        print("Active model mismatch. Testing setup (this might take a moment)...")
        # We will limit the scope by mocking or just running it. 
        # Since it's a real DB, let's run it but maybe it's fast enough or we just trust it calls the right things.
        # Ideally we don't want to re-embed EVERYTHING just for a quick test if it's huge.
        # But per the user request, we need to verify it.
        retriever.setup_vector_index(force_rebuild=True)
        
        # Check again
        new_active = retriever.get_active_model()
        print(f"New Active Model in DB: {new_active}")
        
        if new_active == test_model:
            print("SUCCESS: Model updated correctly.")
        else:
            print("FAILURE: Model was not updated.")
    else:
        print("Active model matches. System is consistent.")
        # Force a "fake" update to ensure write works? 
        # No, that's fine.
    
    retriever.close()
    print("--- Verification Complete ---")

if __name__ == "__main__":
    main()
