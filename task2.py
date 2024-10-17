from pprint import pprint
from DbConnector import DbConnector
from pymongo import MongoClient
from datetime import datetime
from haversine import haversine, Unit  # Import haversine from the library
from tabulate import tabulate

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
    
    def avg_activities_per_user(self):
        # Count the total number of users
        user_count = self.db['User'].count_documents({})

        # Count the total number of activities
        activity_count = self.db['Activity'].count_documents({})

        # Calculate the average activities per user
        avg_activities = activity_count / user_count if user_count > 0 else 0

        return avg_activities

    def top_20_users_with_most_activities(self):
        # MongoDB aggregation to find the top 20 users with the most activities
        pipeline = [
            {
                '$group': {
                    '_id': '$user_id',  # Group by user_id
                    'activity_count': {'$sum': 1}  # Count activities per user
                }
            },
            {
                '$sort': {'activity_count': -1}  # Sort by activity count in descending order
            },
            {
                '$limit': 20  # Limit the result to top 20 users
            }
        ]

        top_20_users = list(self.db['Activity'].aggregate(pipeline))

        # Format data for tabulate
        table = [(user['_id'], user['activity_count']) for user in top_20_users]
        headers = ['User ID', 'Activity Count']

        # Print the result using tabulate
        print("\nTop 20 Users with Most Activities:")
        print(tabulate(table, headers, tablefmt="pretty"))

        return top_20_users
    

    def find_taxi_users(self):
        # MongoDB query to find distinct users who have used 'taxi'
        taxi_users = self.db['Activity'].distinct('user_id', {'transportation_mode': 'taxi'})
        
        if taxi_users:
            print("\nUsers who have taken a taxi:")
            # Print users in a tabulated format
            table = [(user,) for user in taxi_users]
            print(tabulate(table, headers=["User ID"], tablefmt="pretty"))
        else:
            print("No users have taken a taxi.")


    def count_transportation_modes(self):
        # MongoDB aggregation to count transportation modes, excluding 'unknown' and null values
        pipeline = [
            {
                '$match': {
                    'transportation_mode': {'$ne': 'unknown', '$ne': None}  # Filter out 'unknown' and null modes
                }
            },
            {
                '$group': {
                    '_id': '$transportation_mode',  # Group by transportation_mode
                    'mode_count': {'$sum': 1}  # Count the occurrences of each mode
                }
            },
            {
                '$sort': {'mode_count': -1}  # Sort by mode_count in descending order
            }
        ]

        modes = list(self.db['Activity'].aggregate(pipeline))

        if modes:
            print("\nTransportation modes and their activity counts (sorted by count):")
            # Format the data for tabulate
            table = [(mode['_id'], mode['mode_count']) for mode in modes]
            print(tabulate(table, headers=["Transportation Mode", "Count"], tablefmt="pretty"))
        else:
            print("No transportation modes found.")

    def find_year_with_most_activities_and_hours(self):
        # Find the year with the most activities
        pipeline_activities = [
            {
                '$group': {
                    '_id': {'$year': '$start_date_time'},  # Group by year
                    'activity_count': {'$sum': 1}  # Count activities per year
                }
            },
            {
                '$sort': {'activity_count': -1}  # Sort by activity count in descending order
            },
            {
                '$limit': 1  # Get the year with the most activities
            }
        ]

        most_activities_year = list(self.db['Activity'].aggregate(pipeline_activities))

        # Find the year with the most recorded hours using $dateDiff
        pipeline_hours = [
            {
                '$group': {
                    '_id': {'$year': '$start_date_time'},  # Group by year
                    'total_hours': {
                        '$sum': {
                            '$dateDiff': {
                                'startDate': '$start_date_time',
                                'endDate': '$end_date_time',
                                'unit': 'hour'
                            }
                        }
                    }
                }
            },
            {
                '$sort': {'total_hours': -1}  # Sort by total hours in descending order
            },
            {
                '$limit': 1  # Get the year with the most hours
            }
        ]

        most_hours_year = list(self.db['Activity'].aggregate(pipeline_hours))

        if most_activities_year and most_hours_year:
            activity_year = most_activities_year[0]['_id']
            activity_count = most_activities_year[0]['activity_count']

            hours_year = most_hours_year[0]['_id']
            total_hours = most_hours_year[0]['total_hours']

            print(f"Year with most activities: {activity_year} (Activities: {activity_count})")
            print(f"Year with most recorded hours: {hours_year} (Total Hours: {total_hours:.2f})")

            if activity_year == hours_year:
                print(f"Yes, {activity_year} is also the year with the most recorded hours.")
            else:
                print(f"No, the year with the most recorded hours is {hours_year}.")
        else:
            print("No activities found.")
            
    def distance_walked(self):
        # Connect to MongoDB

        # Query for activities of user 112, walking, in 2008
        year_start = datetime(2008, 1, 1)
        year_end = datetime(2008, 12, 31, 23, 59, 59)

        activities = self.db['Activity'].find({
            "user_id": "112",
            "transportation_mode": "walk",
            "start_date_time": {"$gte": year_start, "$lt": year_end}
        })

        # Calculate the total distance walked
        total_distance = 0

        for activity in activities:
            trackpoint_ids = activity.get('trackpoint_ids', [])
            
            # Fetch all trackpoints for the current activity
            trackpoints = self.db['TrackPoint'].find({
                "_id": {"$in": trackpoint_ids}
            }).sort("date_time", 1)  # Sort by date_time to ensure sequential order

            # Initialize variables to store the previous latitude and longitude
            prev_lat = None
            prev_lon = None

            # Iterate through trackpoints and compute distances
            for trackpoint in trackpoints:
                lat = trackpoint['lat']
                lon = trackpoint['lon']

                if prev_lat is not None and prev_lon is not None:
                    # Calculate distance using the haversine library
                    distance = haversine((prev_lat, prev_lon), (lat, lon), unit=Unit.KILOMETERS)
                    total_distance += distance

                # Update previous trackpoint to current one
                prev_lat = lat
                prev_lon = lon

        print(f"Total distance walked by user 112 in 2008: {round(total_distance,2)} km")
        return total_distance


    
def main():
    program = None
    try:
        program = Task_2_Program()
        
        # Show collections
        program.show_collections()

        #Count users, activities, and trackpoints
        user_count, activity_count, trackpoint_count = program.count_users_activities_trackpoints()
        print(f"User Count: {user_count}, Activity Count: {activity_count}, TrackPoint Count: {trackpoint_count}")
        average = program.avg_activities_per_user()
        print(f"Average activities per user: {average}")
        program.top_20_users_with_most_activities()
        program.find_taxi_users()
        program.count_transportation_modes()
        program.find_year_with_most_activities_and_hours()
        program.distance_walked()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
