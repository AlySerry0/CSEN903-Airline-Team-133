import re
from typing import Dict, List

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI


class GraphRAGExperiment:
    def __init__(self, github_token: str, hf_token: str = None):
        """
        Initialize 3 models using GitHub Models (stable & free with Pro).
        We compare: High-End (GPT-4o) vs Reasoning (DeepSeek) vs Efficient (Phi-4).
        """
        # The base URL for all GitHub Models
        self.github_base_url = "https://models.inference.ai.azure.com"

        # --- MODEL 1: GPT-4o (General Purpose High-End) ---
        self.model_gpt4o = ChatOpenAI(
            model="gpt-4o",
            api_key=github_token,
            base_url=self.github_base_url,
            temperature=0
        )

        # --- MODEL 2: DeepSeek-R1 (Reasoning / Chain of Thought) ---
        self.model_deepseek = ChatOpenAI(
            model="DeepSeek-R1",
            api_key=github_token,
            base_url=self.github_base_url,
            temperature=0.6  # Reasoning models often benefit from slight temp
        )

        # --- MODEL 3: Phi-4 (Efficient / Small Language Model) ---
        # Replacing the broken Hugging Face model with Microsoft's Phi-4
        self.model_phi = ChatOpenAI(
            model="Phi-4-multimodal-instruct",
            api_key=github_token,
            base_url=self.github_base_url,
            temperature=0
        )

        self.models = {
            "GPT-4o": self.model_gpt4o,
            "DeepSeek-R1": self.model_deepseek,
            "Phi-4": self.model_phi
        }

    def format_context(self, baseline_records: List[dict], vector_records: List[dict]) -> str:
        """
        Merges and formats Neo4j records into a single context string.
        """
        seen = set()
        context_lines = []

        def process_records(records, source_label):
            MAX_RECORDS = 30 # Safety limit to prevents 413 errors
            count = 0
            for record in records:
                if count >= MAX_RECORDS:
                    context_lines.append(f"[{source_label}] ... (More results truncated for size)")
                    break
                content = str(record)
                if content not in seen:
                    seen.add(content)
                    context_lines.append(f"[{source_label}] {content}")
                    count += 1

        process_records(baseline_records, "Exact Match")
        process_records(vector_records, "Semantic Match")

        return "\n".join(context_lines)

    def _clean_deepseek_thought(self, text: str) -> str:
        """
        Optional: Removes the <think>...</think> block from DeepSeek responses 
        so the UI looks cleaner, while keeping the final answer.
        """
        return re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()

    def run_experiment(self, context_str: str, user_query: str, theme_persona: str) -> Dict:
        results = {}

        # CHANGED: Complex Prompt to force model differentiation
        template = """
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

        prompt = ChatPromptTemplate.from_template(template)

        print(f"--- Starting Experiment for Query: {user_query} ---")

        for name, model in self.models.items():
            print(f"Querying {name}...")
            try:
                # Chain: Prompt -> Model -> String Parser
                chain = prompt | model | StrOutputParser()

                response = chain.invoke({
                    "persona": theme_persona,
                    "context": context_str,
                    "question": user_query
                })

                # Specific cleanup for DeepSeek to separate 'Thought' from 'Answer'
                # useful for your qualitative analysis section.
                if name == "DeepSeek-R1":
                    # We store both raw (for analysis) and clean (for UI)
                    clean_response = self._clean_deepseek_thought(response)
                    results[name] = {
                        "status": "success",
                        "response": clean_response,
                        "raw_thought": response
                    }
                else:
                    results[name] = {
                        "status": "success",
                        "response": response
                    }

            except Exception as e:
                print(f"Error with {name}: {e}")
                results[name] = {
                    "status": "error",
                    "error": str(e)
                }

        return results
