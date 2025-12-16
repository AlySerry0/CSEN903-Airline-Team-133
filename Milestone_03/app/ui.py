import streamlit as st
import time
from typing import Dict, Any, List
import json
import pandas as pd
import sys
import os

# Add the project root to sys.path so we can import 'src'
# ui.py is in app/ -> parent is project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- Project Modules ---
from src.config.manager import ConfigManager
from src.logic.nlp import process_user_query
from src.logic.cypher_gen import generate_cypher
from src.data.graph import GraphExecutor
from src.data.vector import JourneyEmbeddingRetriever
from src.llm.client import GraphRAGExperiment

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Graph-RAG Travel Assistant",
    layout="wide"
)

st.title("✈️ Graph-RAG Travel Assistant")
st.caption("Neo4j Knowledge Graph + LLM (Milestone 3)")

# ============================================================
# INITIALIZATION & SECRETS
# ============================================================
@st.cache_resource
def get_config():
    cm = ConfigManager()
    valid, msg = cm.validate()
    if not valid:
        st.error(msg)
        st.stop()
    return cm

cm = get_config()

# Initialize Resources
@st.cache_resource
def init_resources():
    # 1. Graph Executor for Baseline
    executor = GraphExecutor(
        uri=cm.get("NEO4J_URI"),
        auth=(cm.get("NEO4J_USERNAME"), cm.get("NEO4J_PASSWORD"))
    )
    
    # 2. Embedding Retriever
    retriever = JourneyEmbeddingRetriever(
        uri=cm.get("NEO4J_URI"),
        user=cm.get("NEO4J_USERNAME"),
        password=cm.get("NEO4J_PASSWORD"),
        model_name='BAAI/bge-small-en-v1.5' # Matching what was used in the file
    )
    
    # 3. LLM Experiment Layer
    llm_exp = GraphRAGExperiment(
        github_token=cm.get("GITHUB_TOKEN")
    )
    
    return executor, retriever, llm_exp

try:
    executor, retriever, llm_exp = init_resources()
except Exception as e:
    st.error(f"Failed to initialize resources: {e}")
    st.stop()


# ============================================================
# PIPELINE EXECUTION
# ============================================================
def run_real_graph_rag(
    query: str,
    retrieval_mode: str, # "Baseline (Cypher)", "Embeddings", "Hybrid"
    model_name: str
) -> Dict[str, Any]:
    
    results = {
        "intent": None,
        "entities": {},
        "cypher_queries": [],
        "kg_context": {"nodes": [], "baseline_records": [], "vector_records": []},
        "llm_answer": None,
        "raw_llm_results": {}
    }

    # 1. INTENT & NER
    with st.status("Preprocessing Input...", expanded=False) as status:
        st.write("Detecting Intent...")
        intent, entities = process_user_query(query)
        st.write(f"Intent: {intent}")
        st.write("Extracting Entities...")
        st.json(entities)
        
        results["intent"] = intent
        results["entities"] = entities
        status.update(label="Preprocessing Complete", state="complete", expanded=False)

    # 2. RETRIEVAL
    baseline_records = []
    vector_records = []
    
    # A) Baseline
    if retrieval_mode in ["Baseline (Cypher)", "Hybrid"]:
        if intent != "unsupported_query":
            cypher = generate_cypher(intent, entities)
            results["cypher_queries"].append(cypher)
            
            # Execute
            records = executor.execute(cypher)
            baseline_records = records
            results["kg_context"]["baseline_records"] = records
        else:
            results["cypher_queries"].append("// Unsupported Intent - No Cypher Generated")

    # B) Embeddings
    if retrieval_mode in ["Embeddings", "Hybrid"]:
        # We search using the raw query
        vec_results = retriever.search(query, top_k=5)
        # normalize to dict
        vector_records = vec_results
        results["kg_context"]["vector_records"] = vector_records

    # 3. LLM GENERATION
    # Format context
    context_str = llm_exp.format_context(baseline_records, vector_records)
    
    # Decide persona based on intent (simplified mapping)
    persona = "You are a helpful airline assistant."
    if "flight" in intent: persona = "You are a flight operations expert."
    elif "loyalty" in intent or "passenger" in intent: persona = "You are a customer loyalty specialist."
    
    # Reconstruct the prompt for display purposes (Must match client.py)
    # This is a bit duplicative but safer than modifying the client signature rapidly
    prompt_template = """
        [PERSONA]
        {persona}

        [CONTEXT]
        {context}

        [TASK]
        Answer the user's question: "{question}"

        You MUST follow this exact format:
        **Direct Answer:** [Your concise answer here]
        **Source:** [Mention which specific data point you used]
        **Confidence:** [High/Medium/Low] - Explain why.

        CRITICAL RULES:
        1. If the context has conflicting info, point it out.
        2. If the context is missing the answer, say "I do not know" (Confidence: Low).
        """
    results["final_prompt"] = prompt_template.format(persona=persona, context=context_str, question=query)

    ll_results = llm_exp.run_experiment(context_str, query, persona)
    results["raw_llm_results"] = ll_results
    
    # Extract the answer for the selected model
    # Note: The UI separates model selection, but run_experiment runs ALL 3.
    # We will pick the one the user asked for to display primarily.
    
    # Map UI model name to internal key if needed
    # The UI options: ["GPT-4o", "DeepSeek-R1", "Phi-4"] matching keys in llm_layer
    selected_res = ll_results.get(model_name, {"response": "Error: Model not found"})
    if isinstance(selected_res, dict):
         results["llm_answer"] = selected_res.get("response", str(selected_res))
    else:
         results["llm_answer"] = str(selected_res)

    return results

# ============================================================
# SIDEBAR CONTROLS
# ============================================================
st.sidebar.header("⚙️ Experiment Settings")

retrieval_mode = st.sidebar.selectbox(
    "Retrieval Method",
    ["Hybrid", "Baseline (Cypher)", "Embeddings"]
)

# Available models in llm_layer: GPT-4o, DeepSeek-R1, Phi-4
model_name = st.sidebar.selectbox(
    "Primary LLM Model",
    ["GPT-4o", "DeepSeek-R1", "Phi-4"]
)

st.sidebar.markdown("---")
st.sidebar.info(f"System Status:\n\nNeo4j: {'🟢 Online' if executor.verify_connection() else '🔴 Offline'}")

# ============================================================
# MAIN INPUT
# ============================================================
# --- Prepared Questions ---
PREPARED_QUESTIONS = [
    "Custom...",
    "How does food satisfaction vary by flight?",         # satisfaction_insights
    "List flights from LAX to IAX",                      # find_flights
    "Which flights have the highest delays?",            # flight_performance
    "Show details for Business class journeys",          # journey_details
    "Show passenger distribution by loyalty tier",       # loyalty_insights
    "Show passenger distribution by generation",         # generation_insights
    "How many flights arrive at LAX?",                   # airport_stats
    "Compare flight delays",                             # compare_entities
    "Top 5 flights by food satisfaction"                 # aggregation
]

def update_query_text():
    selection = st.session_state.get("question_selector", "Custom...")
    if selection != "Custom...":
        st.session_state.query_input = selection

st.selectbox(
    "💡 Choose a prepared question:",
    PREPARED_QUESTIONS,
    key="question_selector",
    on_change=update_query_text
)

if "query_input" not in st.session_state:
    st.session_state.query_input = ""

query = st.text_input(
    "Ask a question about flights, routes, or delays",
    key="query_input",
    placeholder="e.g. Which flights from LAX to IAX have the highest delays?"
)

run_clicked = st.button("🚀 Run Graph-RAG")

# ============================================================
# EXECUTION
# ============================================================
if run_clicked:
    if not query.strip():
        st.error("Please enter a question.")
    else:
        with st.spinner("Running pipeline..."):
            try:
                result = run_real_graph_rag(
                    query=query,
                    retrieval_mode=retrieval_mode,
                    model_name=model_name
                )

                col1, col2 = st.columns(2)

                # ---------------- LEFT: KG CONTEXT ----------------
                with col1:
                    st.subheader("🔗 Knowledge Graph Context")
                    
                    if retrieval_mode in ["Baseline (Cypher)", "Hybrid"]:
                        with st.expander(f"� Baseline Results ({len(result['kg_context']['baseline_records'])})"):
                            st.json(result['kg_context']['baseline_records'])

                    if retrieval_mode in ["Embeddings", "Hybrid"]:
                        with st.expander(f"🧠 Vector Results ({len(result['kg_context']['vector_records'])})"):
                            # simplify for display
                            st.write(result['kg_context']['vector_records'])

                # ---------------- RIGHT: META INFO ----------------
                with col2:
                    st.subheader("🧠 Query Understanding")

                    st.markdown(f"**Detected Intent:** `{result['intent']}`")

                    with st.expander("🏷️ Extracted Entities"):
                        st.json(result["entities"])

                    with st.expander("🧾 Cypher Queries Executed"):
                        for q in result["cypher_queries"]:
                            st.code(q, language="cypher")
                    
                    with st.expander("📝 Final Prompt Sent to LLM"):
                        st.code(result.get("final_prompt", "Prompt not captured"), language="text")
                            
                    # Show comparison if available
                    with st.expander("🤖 Model Comparison (Hidden Thoughts)"):
                         st.write(result["raw_llm_results"])

                st.markdown("---")

                # ---------------- FINAL ANSWER ----------------
                st.subheader(f"✅ Final Answer ({model_name})")
                st.markdown(result["llm_answer"])
                
            except Exception as e:
                st.error(f"Pipeline Error: {e}")
                st.exception(e)

# ============================================================
# FOOTER
# ============================================================
st.markdown("---")
st.caption(
    "Milestone 3 • Graph-RAG Travel Assistant"
)