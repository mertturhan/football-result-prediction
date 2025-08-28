import json
from pathlib import Path
from typing import Set

class ResumeState:
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._done: Set[str] = set()
        self.load()

    def _load(self):
        if self.state_file.exists():
            try:
                data = json.loads(self.state_file.read_text())
                if isinstance(data, dict):
                    self._done = set(data)
            except Exception:
                self._done = set()

    def save(self):
        self.state_file.write_text(json.dumps(sorted(self._done)

    
    def is_done(self, key: str) -> bool:
        return key in self._done
    
    def mark_done(self, key: str):
        self._done.add(key)
        self.save()
