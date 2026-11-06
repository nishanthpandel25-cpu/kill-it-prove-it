
from src.database import Database

# Create database
db = Database()

# 1. Store information
db.put(b"name", b"Nishanth")
db.put(b"age", b"20")
db.put(b"college", b"SIT")

print("Data stored successfully!")

# 2. Retrieve information
print("Name:", db.get(b"name"))
print("Age:", db.get(b"age"))

# 3. Update information
db.put(b"age", b"21")

print("Updated age:", db.get(b"age"))

# 4. Delete information
db.delete(b"college")

print("College:", db.get(b"college"))
