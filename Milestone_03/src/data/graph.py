from neo4j import GraphDatabase
from typing import List, Dict, Any

class GraphExecutor:
    """
    Executes Cypher queries against the Neo4j database.
    Used for the Baseline retrieval strategy.
    """
    def __init__(self, uri: str, auth: tuple):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()

    def execute(self, query: str, parameters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        if not query or query.strip().startswith("// Is unsupported"):
            return []

        with self.driver.session() as session:
            try:
                result = session.run(query, parameters or {})
                return [record.data() for record in result]
            except Exception as e:
                print(f"Query Execution Error: {e}")
                print(f"Query: {query}")
                return [{"error": str(e)}]

    def verify_connection(self) -> bool:
        try:
            self.driver.verify_connectivity()
            return True
        except Exception as e:
            print(f"Connection verification failed: {e}")
            return False
