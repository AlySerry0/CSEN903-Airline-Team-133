import os

from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer

MODEL_CONFIGS = {
    'all-MiniLM-L6-v2': {'dim': 384, 'normalize': False},
    'BAAI/bge-small-en-v1.5': {'dim': 384, 'normalize': True},
}


class InputProcessor:
    def __init__(self, model_name):
        if model_name not in MODEL_CONFIGS:
            raise ValueError(f"Model {model_name} not supported in configuration.")

        print(f"Loading Embedding Model: {model_name}...")
        self.model = SentenceTransformer(model_name)
        self.model_name = model_name
        self.normalize = MODEL_CONFIGS[model_name]['normalize']

    def embed_input(self, user_text):
        """
        Converts user text into a vector representation.
        """
        # SentenceTransformer handles normalization if specified
        vector = self.model.encode(user_text, normalize_embeddings=self.normalize)
        return vector.tolist()


class JourneyEmbeddingRetriever:
    def __init__(self, uri, user, password, model_name='all-MiniLM-L6-v2'):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.model_name = model_name

        # Determine dimensions dynamically
        if model_name in MODEL_CONFIGS:
            self.dimension = MODEL_CONFIGS[model_name]['dim']
        else:
            # Fallback or default
            print(f"Warning: Unknown model dimensions for {model_name}. Defaulting to 384.")
            self.dimension = 384

        self.processor = InputProcessor(model_name)

    def close(self):
        self.driver.close()

    def setup_vector_index(self, force_rebuild=False):
        """
        Orchestrates the setup: prepares text, embeds, and indexes.
        Call this with force_rebuild=True only when initializing the DB or switching models.
        """
        if not force_rebuild:
            print("Skipping index rebuild (force_rebuild=False).")
            return

        print(f"\n[SETUP] Starting setup for model: {self.model_name}...")
        self._prepare_text_nodes()
        self._generate_and_store_embeddings()
        self._create_index()
        print("[SETUP] Complete.\n")

    def _prepare_text_nodes(self):
        """Internal: Creates text representation on nodes."""
        print(" -> Updating text representations on Journey nodes...")
        query = """
        MATCH (p:Passenger)-[:TOOK]->(j:Journey)-[:ON]->(f:Flight)
        MATCH (f)-[:DEPARTS_FROM]->(origin:Airport)
        MATCH (f)-[:ARRIVES_AT]->(dest:Airport)
        SET j.text_representation =
            "Flight Number: " + toString(f.flight_number) + ". " +
            "Route: " + origin.station_code + " to " + dest.station_code + ". " +
            "Aircraft: " + f.fleet_type_description + ". " +
            "Distance: " + toString(j.actual_flown_miles) + " miles with " + toString(j.number_of_legs) + " legs. " +
            "Passenger: " + p.generation + " generation, " + p.loyalty_program_level + " member. " +
            "Class: " + j.passenger_class + ". " +
            "Feedback: Food rating " + toString(j.food_satisfaction_score) + "/5. " +
            "Arrival delay: " + toString(j.arrival_delay_minutes) + " minutes."
        """
        with self.driver.session() as session:
            session.run(query)

    def _generate_and_store_embeddings(self):
        """Internal: Embeds the text representations."""
        print(" -> Generating embeddings...")
        fetch_query = """
        MATCH (j:Journey)
        WHERE j.text_representation IS NOT NULL
        RETURN elementId(j) AS id, j.text_representation AS text
        """
        update_query = """
        MATCH (j:Journey)
        WHERE elementId(j) = $id
        SET j.embedding = $vector
        """

        with self.driver.session() as session:
            results = session.run(fetch_query).data()
            print(f"    Found {len(results)} journeys to embed.")

            batch_size = 200
            for i in range(0, len(results), batch_size):
                batch = results[i:i + batch_size]
                texts = [r['text'] for r in batch]
                ids = [r['id'] for r in batch]

                # Use the processor to ensure consistency
                vectors = self.processor.model.encode(texts, normalize_embeddings=self.processor.normalize)

                for k, vector in enumerate(vectors):
                    session.run(update_query, id=ids[k], vector=vector.tolist())

                # Optional: Simple progress indicator
                if i % 1000 == 0:
                    print(f"    Processed {i}/{len(results)}...")

    def _create_index(self):
        """Internal: Creates the vector index with the correct dimensions."""
        print(f" -> Creating/Recreating Index 'journey_embeddings' (Dim: {self.dimension})...")

        # Note: We drop the index if it exists to ensure dimensions match the current model
        drop_query = "DROP INDEX journey_embeddings IF EXISTS"

        index_query = f"""
        CREATE VECTOR INDEX journey_embeddings IF NOT EXISTS
        FOR (j:Journey)
        ON (j.embedding)
        OPTIONS {{indexConfig: {{
         `vector.dimensions`: {self.dimension},
         `vector.similarity_function`: 'cosine'
        }}}}
        """
        with self.driver.session() as session:
            session.run(drop_query)
            session.run(index_query)

    def search(self, query_text, top_k=3):
        """
        Public method to be called by the UI/Orchestrator.
        Returns a list of results (dicts).
        """
        query_vector = self.processor.embed_input(query_text)

        cypher = """
        CALL db.index.vector.queryNodes('journey_embeddings', $k, $query_vector)
        YIELD node, score
        RETURN node.text_representation AS context, score, elementId(node) as id
        """

        with self.driver.session() as session:
            result = session.run(cypher, k=top_k, query_vector=query_vector)
            return [record.data() for record in result]


def load_config(file_path='config.txt'):
    config = {}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        config[key] = value.strip()
            return config
        except Exception:
            pass
    return {'URI': "bolt://localhost:7687", 'USERNAME': "neo4j", 'PASSWORD': "password"}


if __name__ == "__main__":
    cfg = load_config()

    retriever = JourneyEmbeddingRetriever(
        cfg['URI'], cfg['USERNAME'], cfg['PASSWORD'],
        model_name='BAAI/bge-small-en-v1.5'
    )

    retriever.setup_vector_index(force_rebuild=False)

    results = retriever.search("Flights with bad food", top_k=20)

    for r in results:
        print(f"Score: {r['score']:.4f} | {r['context']}")

    retriever.close()
