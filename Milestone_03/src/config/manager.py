import os
from typing import Dict

try:
    import streamlit as st
except ImportError:
    st = None

class ConfigManager:
    """
    Centralized configuration handler for the Airline Graph-RAG system.
    Prioritizes:
    1. Streamlit Secrets (st.secrets)
    2. Environment Variables
    3. config.txt (Legacy/Local)
    """
    
    REQUIRED_KEYS = [
        "GITHUB_TOKEN",
        "NEO4J_URI",
        "NEO4J_USERNAME",
        "NEO4J_PASSWORD"
    ]

    def __init__(self):
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, str]:
        config = {}
        
        # 1. Load from config.txt (Primary)
        # We look in the current directory and also check parent dir just in case
        candidates = ['config.txt', os.path.join(os.path.dirname(__file__), 'config.txt')]
        
        file_conf = {}
        for path in candidates:
            if os.path.exists(path):
                file_conf = self._load_from_file(path)
                if file_conf:
                    break
        
        # Normalize Keys from config.txt
        # Map: URI -> NEO4J_URI, USERNAME -> NEO4J_USERNAME, PASSWORD -> NEO4J_PASSWORD
        if 'URI' in file_conf: config['NEO4J_URI'] = file_conf['URI']
        elif 'NEO4J_URI' in file_conf: config['NEO4J_URI'] = file_conf['NEO4J_URI']
            
        if 'USERNAME' in file_conf: config['NEO4J_USERNAME'] = file_conf['USERNAME']
        elif 'NEO4J_USERNAME' in file_conf: config['NEO4J_USERNAME'] = file_conf['NEO4J_USERNAME']
            
        if 'PASSWORD' in file_conf: config['NEO4J_PASSWORD'] = file_conf['PASSWORD']
        elif 'NEO4J_PASSWORD' in file_conf: config['NEO4J_PASSWORD'] = file_conf['NEO4J_PASSWORD']
            
        if 'GITHUB_TOKEN' in file_conf: config['GITHUB_TOKEN'] = file_conf['GITHUB_TOKEN']

        # 2. Fallback: Environment Variables (if missing in file)
        for key in self.REQUIRED_KEYS:
            if key not in config:
                val = os.getenv(key)
                if val:
                    config[key] = val
                    
        # 3. Fallback: Streamlit Secrets (lowest priority now, or removed if strictly single file)
        # Keeping as last resort fallback for graceful degradation
        if st is not None:
            try:
                for key in self.REQUIRED_KEYS:
                    if key not in config and key in st.secrets:
                        config[key] = st.secrets[key]
            except Exception:
                pass

        return config

    def _load_from_file(self, file_path: str) -> Dict[str, str]:
        c = {}
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    for line in f:
                        if '=' in line:
                            k, v = line.strip().split('=', 1)
                            c[k.strip()] = v.strip().strip('"').strip("'")
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
        return c

    def get(self, key: str, default=None):
        return self.config.get(key, default)

    def validate(self):
        missing = [k for k in self.REQUIRED_KEYS if k not in self.config]
        if missing:
            return False, f"Missing configuration keys: {', '.join(missing)}"
        return True, "Configuration valid."
