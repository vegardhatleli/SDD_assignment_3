from pprint import pprint
from DbConnector import DbConnector
from pymongo import MongoClient
from datetime import datetime
from haversine import haversine, Unit  # Import haversine from the library
from tabulate import tabulate
from collections import defaultdict
from tqdm import tqdm

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

    def get_most_used_transportation_mode(self):
            print("Finding users with their most used transportation mode...")

            # MongoDB Aggregation Query
            pipeline = [
                {"$match": {"transportation_mode": {"$ne": None}}},  # Filter out documents with null transportation_mode
                {
                    "$group": {
                        "_id": {
                            "user_id": "$user_id",
                            "transportation_mode": "$transportation_mode"
                        },
                        "mode_count": {"$sum": 1}  # Count occurrences of each transportation_mode per user
                    }
                },
                {
                    "$sort": {
                        "_id.user_id": 1,  # Sort by user_id (ascending)
                        "mode_count": -1    # Sort by mode_count (descending) for each user
                    }
                }
            ]

            result = self.db['Activity'].aggregate(pipeline)
            
            most_used_modes = defaultdict(lambda: (None, 0))  # Dictionary to store most used modes for each user

            # Process the aggregated results
            for doc in result:
                user_id = doc['_id']['user_id']
                transportation_mode = doc['_id']['transportation_mode']
                mode_count = doc['mode_count']
                
                # Update the dictionary with the mode that has the highest count for each user
                if mode_count > most_used_modes[user_id][1]:
                    most_used_modes[user_id] = (transportation_mode, mode_count)
            
            # Display results in a table format
            if most_used_modes:
                table_data = [[user_id, mode] for user_id, (mode, _) in sorted(most_used_modes.items())]
                table = tabulate(table_data, headers=["User ID", "Most Used Transportation Mode"], tablefmt="pretty")
                print("\nMost used transportation modes per user:")
                print(table)
            else:
                print("No transportation mode data available.")

    def top_20_users_by_altitude_gain(self):
        # Dictionary to store altitude gains for each user
        user_altitude_gain = {}

        # Fetch activities where trackpoints are available (ignore activities without trackpoints)
        activities = self.db['Activity'].find({'trackpoint_ids': {'$exists': True, '$not': {'$size': 0}}})

        # Process each activity
        for activity in tqdm(activities, desc="Processing activities", unit="activity"):
            user_id = activity['user_id']
            trackpoint_ids = activity['trackpoint_ids']

            # Retrieve all trackpoints for this activity in a sorted order
            trackpoints = list(self.db['TrackPoint'].find({'_id': {'$in': trackpoint_ids}}).sort('date_time', 1))

            # Iterate through trackpoints and compute altitude gains
            for i in range(1, len(trackpoints)):
                current_altitude = trackpoints[i]['altitude']
                previous_altitude = trackpoints[i - 1]['altitude']

                # Check if altitude is valid and increasing
                if current_altitude != -777 and current_altitude > previous_altitude:
                    altitude_gain = (current_altitude - previous_altitude) * 0.3048  # Convert feet to meters
                    if user_id in user_altitude_gain:
                        user_altitude_gain[user_id] += altitude_gain
                    else:
                        user_altitude_gain[user_id] = altitude_gain

        # Sort the users by total altitude gain and take the top 20
        top_20_users = sorted(user_altitude_gain.items(), key=lambda x: x[1], reverse=True)[:20]

        # Display the results
        if top_20_users:
            print("Top 20 Users by Altitude Gain (User ID, Total Meters Gained):")
            print(tabulate(top_20_users, headers=["User ID", "Total Meters Gained"], tablefmt="grid"))
        else:
            print("No altitude data found.")


    def find_users_in_forbidden_city(self):
        # Exact coordinates for the Forbidden City
        forbidden_city_lat = 39.916
        forbidden_city_lon = 116.397
        tolerance = 0.001  # Adjust tolerance as necessary

        # Define the latitude and longitude range for the forbidden city with tolerance
        lat_min = forbidden_city_lat
        lat_max = forbidden_city_lat + tolerance
        lon_min = forbidden_city_lon
        lon_max = forbidden_city_lon + tolerance

        # Step 1: Find trackpoints within the latitude/longitude range
        matching_trackpoints = self.db['TrackPoint'].find({
            "lat": {"$gte": lat_min, "$lte": lat_max},
            "lon": {"$gte": lon_min, "$lte": lon_max}
        }, {"_id": 1})  # Only return trackpoint ids

        # Step 2: Extract the trackpoint ids
        trackpoint_ids = [tp['_id'] for tp in matching_trackpoints]

        if not trackpoint_ids:
            print("No trackpoints found within the Forbidden City area.")
            return

        # Step 3: Find activities that include these trackpoint ids
        activities_with_matching_trackpoints = self.db['Activity'].find({
            "trackpoint_ids": {"$in": trackpoint_ids}
        }, {"user_id": 1})  # Only return user ids

        # Step 4: Extract distinct user IDs
        user_ids = {activity['user_id'] for activity in activities_with_matching_trackpoints}

        # Step 5: Output the result
        if user_ids:
            print("Users who have tracked an activity in the Forbidden City (with tolerance):")
            for user_id in user_ids:
                print(f"User ID: {user_id}")
        else:
            print("No users found who have tracked an activity in the Forbidden City.")


def main():
    program = None
    try:
        program = Task_2_Program()
        
        # Show collections
        program.show_collections()
        '''
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
        '''
        #program.top_20_users_by_altitude_gain()
        program.find_users_in_forbidden_city()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
