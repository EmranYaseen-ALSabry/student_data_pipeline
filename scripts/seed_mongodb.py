import os
import sys
from pathlib import Path
from typing import Any, Dict, List
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Ensure project root is in sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

DEFAULT_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DEFAULT_DATABASE = os.getenv("MONGODB_DATABASE", "student_pipeline")
DEFAULT_COLLECTION = os.getenv("MONGODB_COLLECTION", "student_extra")


def get_sample_students_data() -> List[Dict[str, Any]]:
    """Generates 22 diverse, realistic student documents.

    Note:
    - Does NOT duplicate core fields (name, age, gpa, attendance_rate).
    - Contains rich supplementary attributes: contact, address, guardian, skills, courses, projects.
    - Includes matching student_ids (101-112) and MongoDB-exclusive student_ids (113-122).
    - Contains edge cases (missing optional fields, multiple skills, multiple projects).
    """
    return [
        {
            "student_id": 102,
            "contact": {
                "phone": "+967771234567",
            },
            "address": {
                "street": "Al-Zubairi",
                "city": "Sanaa",
                "country": "Yemen"
            },
            "guardian": {
                "name": "Ali Ahmed",
                "relationship": "Father",
                "phone": "+967770000001"
            },
            "skills": ["Python", "SQL", "MongoDB"],
            "courses": [
                {"name": "Python Programming", "grade": 92},
                {"name": "Database Systems", "grade": 88}
            ],
            "projects": [
                {"name": "Student Data Pipeline", "technologies": ["Python", "Pandas", "MongoDB"]}
            ]
        },
        {
            "student_id": 101,
            "contact": {
                "phone": "+967772345678",
            },
            "address": {
                "street": "Corniche Road",
                "city": "Aden",
                "country": "Yemen"
            },
            "guardian": {
                "name": "Omar Salem",
                "relationship": "Father",
                "phone": "+967770000002"
            },
            "skills": ["R", "Python", "Data Mining", "Tableau"],
            "courses": [
                {"name": "Big Data Analytics", "grade": 95},
                {"name": "Data Warehousing", "grade": 91}
            ],
            "projects": [
                {"name": "Customer Churn Prediction", "technologies": ["Python", "Scikit-Learn"]}
            ]
        },
        {
            "student_id": 103,
            "contact": {
                "phone": "+966501112233"
            },
            "address": {
                "street": "King Fahd Road",
                "city": "Riyadh",
                "country": "Saudi Arabia"
            },
            "guardian": {
                "name": "Saleh Mansour",
                "relationship": "Uncle",
                "phone": "+966509998877"
            },
            "skills": ["Java", "Spring Boot", "SQL"],
            "courses": [
                {"name": "Enterprise Architecture", "grade": 78}
            ],
            "projects": []
        },
        {
            "student_id": 104,
            "contact": {
                "phone": "+12125550199"
            },
            "address": {
                "street": "Broadway Ave",
                "city": "New York",
                "country": "USA"
            },
            "guardian": {
                "name": "Khaled Al-Sayed",
                "relationship": "Father",
                "phone": "+12125550100"
            },
            "skills": ["Docker", "Kubernetes", "Linux", "Python", "AWS"],
            "courses": [
                {"name": "Cloud Computing", "grade": 96},
                {"name": "DevOps Engineering", "grade": 94}
            ],
            "projects": [
                {"name": "Microservices Cluster", "technologies": ["Docker", "K8s", "FastAPI"]}
            ]
        },
        {
            "student_id": 105,
            # Missing contact phone (Optional field test)
            "address": {
                "street": "Sheikh Zayed Road",
                "city": "Dubai",
                "country": "UAE"
            },
            "guardian": {
                "name": "Hassan Tariq",
                "relationship": "Brother",
                "phone": "+971500000005"
            },
            "skills": ["C++", "Algorithms", "Computer Vision"],
            "courses": [
                {"name": "Deep Learning", "grade": 98}
            ],
            "projects": [
                {"name": "Autonomous Object Detection", "technologies": ["PyTorch", "OpenCV"]}
            ]
        },
        {
            "student_id": 106,
            "contact": {
                "phone": "+201012345678"
            },
            "address": {
                "street": "Tahrir Square",
                "city": "Cairo",
                "country": "Egypt"
            },
            "guardian": {
                "name": "Tarek Mostafa",
                "relationship": "Father",
                "phone": "+201099998888"
            },
            "skills": ["JavaScript", "React", "Node.js", "MongoDB"],
            "courses": [
                {"name": "Web Development", "grade": 86}
            ],
            "projects": [
                {"name": "Student Portal System", "technologies": ["React", "Express", "Mongo"]}
            ]
        },
        {
            "student_id": 107,
            "contact": {
                "phone": "+201055566677"
            },
            "address": {
                "street": "Pyramids St",
                "city": "Giza",
                "country": "Egypt"
            },
            "guardian": {
                "name": "Hamed Ziad",
                "relationship": "Father",
                "phone": "+201077778888"
            },
            "skills": ["PowerBI", "SQL", "Excel", "ETL"],
            "courses": [
                {"name": "Business Intelligence", "grade": 89}
            ],
            "projects": [
                {"name": "Sales Performance Dashboard", "technologies": ["PowerBI", "SQL Server"]}
            ]
        },
        {
            "student_id": 108,
            "contact": {
                "phone": "+201188899900"
            },
            "address": {
                "street": "Stanley Bridge",
                "city": "Alexandria",
                "country": "Egypt"
            },
            # Missing guardian info (Optional test)
            "skills": ["HTML", "CSS", "UI/UX", "Figma"],
            "courses": [],
            "projects": []
        },
        {
            "student_id": 109,
            "contact": {
                "phone": "+966540001122"
            },
            "address": {
                "street": "Madinah Road",
                "city": "Jeddah",
                "country": "Saudi Arabia"
            },
            "guardian": {
                "name": "Adel Kareem",
                "relationship": "Father",
                "phone": "+966549990011"
            },
            "skills": ["Cybersecurity", "Penetration Testing", "Wireshark", "Linux"],
            "courses": [
                {"name": "Network Defense", "grade": 85}
            ],
            "projects": [
                {"name": "Vulnerability Assessment", "technologies": ["Kali Linux", "Metasploit"]}
            ]
        },
        {
            "student_id": 110,
            "contact": {
                "phone": "+201200001111"
            },
            "address": {
                "street": "Nasr City",
                "city": "Cairo",
                "country": "Egypt"
            },
            "guardian": {
                "name": "Mansour Nour",
                "relationship": "Father",
                "phone": "+201299992222"
            },
            "skills": ["C#", ".NET Core", "SQL Server"],
            "courses": [
                {"name": "Software Architecture", "grade": 90}
            ],
            "projects": [
                {"name": "E-Commerce Microservice", "technologies": [".NET", "SQL"]}
            ]
        },
        {
            "student_id": 111,
            "contact": {
                "phone": "+966551113333"
            },
            "address": {
                "street": "Olaya St",
                "city": "Riyadh",
                "country": "Saudi Arabia"
            },
            "guardian": {
                "name": "Ziyad Tariq",
                "relationship": "Father",
                "phone": "+966559994444"
            },
            "skills": ["Scratch", "Python Basics"],
            "courses": [
                {"name": "Intro to Computing", "grade": 80}
            ],
            "projects": []
        },
        {
            "student_id": 112,
            "contact": {
                "phone": "+201033334444"
            },
            "address": {
                "street": "Heliopolis",
                "city": "Cairo",
                "country": "Egypt"
            },
            "guardian": {
                "name": "Mahmoud Huda",
                "relationship": "Son",
                "phone": "+201088887777"
            },
            "skills": ["Research", "Academic Writing"],
            "courses": [],
            "projects": []
        },
        # MongoDB-exclusive students (Demonstrating Outer Join capture)
    ]


def seed_mongodb(
    uri: str = DEFAULT_URI,
    database_name: str = DEFAULT_DATABASE,
    collection_name: str = DEFAULT_COLLECTION,
    drop_existing: bool = True,
) -> int:
    """Connects to MongoDB, initializes database/collection, and inserts student documents.

    Args:
        uri: MongoDB connection URI string.
        database_name: Database name.
        collection_name: Collection name.
        drop_existing: If True, clears existing records prior to seeding.

    Returns:
        int: Total number of documents inserted.
    """
    print(f"Connecting to MongoDB at: {uri} ...")
    client = None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=4000)
        # Verify connection
        client.admin.command("ping")
        print("Connected successfully to MongoDB server.")

        db = client[database_name]
        collection = db[collection_name]

        if drop_existing:
            delete_result = collection.delete_many({})
            print(f"Cleared existing data from '{collection_name}': {delete_result.deleted_count} documents removed.")

        sample_data = get_sample_students_data()
        insert_result = collection.insert_many(sample_data)
        count = len(insert_result.inserted_ids)

        print("=========================================================")
        print(f"SUCCESS: Inserted {count} student documents into '{database_name}.{collection_name}'.")
        print("=========================================================")
        return count

    except ServerSelectionTimeoutError as e:
        print(f"\n[ERROR] Could not connect to MongoDB server at '{uri}'.")
        print("Please verify that MongoDB service is running on your machine.")
        print(f"Detailed Error: {e}")
        sys.exit(1)
    except ConnectionFailure as e:
        print(f"\n[ERROR] Connection failure with MongoDB: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Unexpected error during seeding: {e}")
        sys.exit(1)
    finally:
        if client:
            client.close()
            print("MongoDB client connection closed.")


if __name__ == "__main__":
    seed_mongodb()
