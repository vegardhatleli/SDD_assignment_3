from pprint import pprint
from DbConnector import DbConnector

class Task_2_Program:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def show_collections(self):
        print("Showing collections:")
        collections = self.db.list_collection_names()
        print(collections)

    def count_users_activities_trackpoints(self):
        user_count = self.db['User'].count_documents({})
        activity_count = self.db['Activity'].count_documents({})
        trackpoint_count = self.db['TrackPoint'].count_documents({})

        return user_count, activity_count, trackpoint_count
    
def main():
    program = None
    try:
        program = Task_2_Program()
        
        # Show collections
        program.show_collections()

        # Count users, activities, and trackpoints
        user_count, activity_count, trackpoint_count = program.count_users_activities_trackpoints()
        print(f"User Count: {user_count}, Activity Count: {activity_count}, TrackPoint Count: {trackpoint_count}")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
