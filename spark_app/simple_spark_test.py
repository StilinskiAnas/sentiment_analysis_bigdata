from cassandra.cluster import Cluster
import uuid

# Connect to Cassandra
cluster = Cluster(['cassandra'])  # Use your Cassandra host
session = cluster.connect()

# Create keyspace
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS spark_streams
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
""")

# Use keyspace
session.execute("USE spark_streams;")

# Create table
session.execute("""
    CREATE TABLE IF NOT EXISTS created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT
    );
""")

# Example data
example_users = [
    {
        'id': uuid.uuid4(),
        'first_name': 'John',
        'last_name': 'Doe',
        'gender': 'Male',
        'address': '123 Main St',
        'post_code': '12345',
        'email': 'john.doe@example.com',
        'username': 'johndoe',
        'registered_date': '2023-11-23',
        'phone': '555-0123',
        'picture': 'http://example.com/pic1.jpg'
    },
    {
        'id': uuid.uuid4(),
        'first_name': 'Jane',
        'last_name': 'Smith',
        'gender': 'Female',
        'address': '456 Oak Ave',
        'post_code': '67890',
        'email': 'jane.smith@example.com',
        'username': 'janesmith',
        'registered_date': '2023-11-23',
        'phone': '555-0124',
        'picture': 'http://example.com/pic2.jpg'
    },
    {
        'id': uuid.uuid4(),
        'first_name': 'Bob',
        'last_name': 'Johnson',
        'gender': 'Male',
        'address': '789 Pine Rd',
        'post_code': '11223',
        'email': 'bob.j@example.com',
        'username': 'bobjohnson',
        'registered_date': '2023-11-23',
        'phone': '555-0125',
        'picture': 'http://example.com/pic3.jpg'
    },
    {
        'id': uuid.uuid4(),
        'first_name': 'Alice',
        'last_name': 'Brown',
        'gender': 'Female',
        'address': '321 Elm St',
        'post_code': '44556',
        'email': 'alice.b@example.com',
        'username': 'aliceb',
        'registered_date': '2023-11-23',
        'phone': '555-0126',
        'picture': 'http://example.com/pic4.jpg'
    },
    {
        'id': uuid.uuid4(),
        'first_name': 'Charlie',
        'last_name': 'Wilson',
        'gender': 'Male',
        'address': '654 Maple Dr',
        'post_code': '77889',
        'email': 'charlie.w@example.com',
        'username': 'charliew',
        'registered_date': '2023-11-23',
        'phone': '555-0127',
        'picture': 'http://example.com/pic5.jpg'
    }
]

# Insert data
insert_statement = """
    INSERT INTO created_users (
        id, first_name, last_name, gender, address, 
        post_code, email, username, registered_date, phone, picture
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for user in example_users:
    session.execute(insert_statement, (
        user['id'],
        user['first_name'],
        user['last_name'],
        user['gender'],
        user['address'],
        user['post_code'],
        user['email'],
        user['username'],
        user['registered_date'],
        user['phone'],
        user['picture']
    ))

# Verify data
rows = session.execute("SELECT * FROM created_users")
print("\nVerifying inserted data:")
for row in rows:
    print(f"ID: {row.id}, Name: {row.first_name} {row.last_name}, Email: {row.email}")

# Close connection
cluster.shutdown()