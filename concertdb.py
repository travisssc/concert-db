from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["project3"]

def state_report():
    pipeline = [
        {"$group": {"_id": "$state", "venueCount": {"$sum": 1}}},
        {"$sort": {"venueCount": -1}}
    ]
    results = db.venues.aggregate(pipeline)
    print("\nState Report:")
    for result in results:
        print(f"State: {result['_id']}, Venues: {result['venueCount']}")

def artist_search():
    artist_name = input("\nEnter artist name: ").strip()
    concerts = db.concerts.find({"performers.artist.name": artist_name}, 
                                {"title": 1, "start": 1, "venue.name": 1, "venue.city": 1, "venue.state": 1})
    print(f"\nConcerts featuring {artist_name}:")
    for concert in concerts:
        print(f"{concert['title']} - {concert['start']} - {concert['venue']['name']} ({concert['venue']['city']}, {concert['venue']['state']})")

def general_admission_totals():
    pipeline = [
        {"$match": {"state": "CA"}},        
        {"$unwind": "$sections"},
        {"$match": {"sections.title": "General Admission"}},
        {"$lookup": {
            "from": "tickets",
            "let": {
                "sectionTitle": "$sections.title", 
                "venueName": "$name"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$seat.sectionTitle", "$$sectionTitle"]},
                                {"$eq": ["$venue.name", "$$venueName"]}
                            ]
                        }
                    }
                }
            ],
            "as": "section_tickets"
        }},
        {"$unwind": "$section_tickets"},
        {"$group": {
            "_id": {"venueName": "$name", "sectionTitle": "$sections.title"},
            "totalSales": {"$sum": "$section_tickets.price"}
        }},
        {"$sort": {"totalSales": -1}}
    ]
    results = db.venues.aggregate(pipeline)
    print("\nGeneral Admission Totals:")
    for result in results:
        venue_name = result["_id"]["venueName"]
        section_title = result["_id"]["sectionTitle"]
        total_sales = result["totalSales"]
        print(f"{venue_name} - {section_title} - ${total_sales}")


def main_menu():
    while True:
        print("\nMain Menu:")
        print("1. State Report")
        print("2. Artist Search")
        print("3. General Admission Totals")
        print("4. Exit")
        choice = input("Enter your choice (1-4): ").strip()
        
        if choice == "1":
            state_report()
        elif choice == "2":
            artist_search()
        elif choice == "3":
            general_admission_totals()
        elif choice == "4":
            print("Exiting the application. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main_menu()
