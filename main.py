from src.database import Database

db = Database("data/database.db")

print("Name:", db.get(b"name"))
print("Age:", db.get(b"age"))
print("College:", db.get(b"college"))

db.close()