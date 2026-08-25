import os
import json
import time


class SimpleDB:

    def __init__(self, db_file="data.db", wal_file="wal.log"):
        self.db_file = db_file
        self.wal_file = wal_file

        # Open WAL in append mode
        self.wal_fd = os.open(
            self.wal_file,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND
        )

        # Recover previous state
        self.state = self._recover_or_load()

    def set(self, key: str, value: str):

        # Create WAL record
        record = json.dumps({
            "key": key,
            "value": value,
            "timestamp": time.time()
        }) + "\n"

        # Step 1: Write record to WAL
        os.write(
            self.wal_fd,
            record.encode("utf-8")
        )

        # Step 2: Guarantee WAL reaches disk
        os.fsync(self.wal_fd)

        # Step 3: Update in-memory database
        self.state[key] = value

    def _recover_or_load(self) -> dict:

        state = {}

        if os.path.exists(self.wal_file):

            with open(self.wal_file, "r") as f:

                for line in f:

                    if line.strip():

                        entry = json.loads(line)

                        state[entry["key"]] = entry["value"]

        return state

    def get(self, key: str):

        return self.state.get(key)

    def close(self):

        os.close(self.wal_fd)


# Example usage

db = SimpleDB()

db.set("name", "Arijit")
db.set("course", "Python")
db.set("goal", "Learn harder")

print(db.get("name"))
print(db.get("course"))
print(db.get("goal"))

db.close()