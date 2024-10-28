from pprint import pprint
from DbConnector import DbConnector
from pymongo import MongoClient
from datetime import datetime
from haversine import haversine, Unit 
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
        user_count = self.db['User'].count_documents({})

        activity_count = self.db['Activity'].count_documents({})

        avg_activities = activity_count / user_count if user_count > 0 else 0

        return avg_activities

    def top_20_users_with_most_activities(self):
        pipeline = [
            {
                '$group': {
                    '_id': '$user_id',  
                    'activity_count': {'$sum': 1}  
                }
            },
            {
                '$sort': {'activity_count': -1}  
            },
            {
                '$limit': 20  
            }
        ]

        top_20_users = list(self.db['Activity'].aggregate(pipeline))

        table = [(user['_id'], user['activity_count']) for user in top_20_users]
        headers = ['User ID', 'Activity Count']

        print("\nTop 20 Users with Most Activities:")
        print(tabulate(table, headers, tablefmt="pretty"))

        return top_20_users
    

    def find_taxi_users(self):
        taxi_users = self.db['Activity'].distinct('user_id', {'transportation_mode': 'taxi'})
        
        if taxi_users:
            print("\nUsers who have taken a taxi:")
            table = [(user,) for user in taxi_users]
            print(tabulate(table, headers=["User ID"], tablefmt="pretty"))
        else:
            print("No users have taken a taxi.")


    def count_transportation_modes(self):
        pipeline = [
            {
                '$match': {
                    'transportation_mode': {'$ne': 'unknown', '$ne': None}  
                }
            },
            {
                '$group': {
                    '_id': '$transportation_mode',  
                    'mode_count': {'$sum': 1}  
                }
            },
            {
                '$sort': {'mode_count': -1}  
            }
        ]

        modes = list(self.db['Activity'].aggregate(pipeline))

        if modes:
            print("\nTransportation modes and their activity counts (sorted by count):")
            table = [(mode['_id'], mode['mode_count']) for mode in modes]
            print(tabulate(table, headers=["Transportation Mode", "Count"], tablefmt="pretty"))
        else:
            print("No transportation modes found.")

    def find_year_with_most_activities_and_hours(self):
        pipeline_activities = [
            {
                '$group': {
                    '_id': {'$year': '$start_date_time'},  
                    'activity_count': {'$sum': 1} 
                }
            },
            {
                '$sort': {'activity_count': -1}  
            },
            {
                '$limit': 1 
            }
        ]

        most_activities_year = list(self.db['Activity'].aggregate(pipeline_activities))

        pipeline_hours = [
            {
                '$group': {
                    '_id': {'$year': '$start_date_time'},  
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
                '$sort': {'total_hours': -1}  
            },
            {
                '$limit': 1 
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

        year_start = datetime(2008, 1, 1)
        year_end = datetime(2008, 12, 31, 23, 59, 59)

        activities = self.db['Activity'].find({
            "user_id": "112",
            "transportation_mode": "walk",
            "start_date_time": {"$gte": year_start, "$lt": year_end}
        })

        total_distance = 0

        for activity in activities:
            trackpoint_ids = activity.get('trackpoint_ids', [])
            
            trackpoints = self.db['TrackPoint'].find({
                "_id": {"$in": trackpoint_ids}
            }).sort("date_time", 1)  

            prev_lat = None
            prev_lon = None

            for trackpoint in trackpoints:
                lat = trackpoint['lat']
                lon = trackpoint['lon']

                if prev_lat is not None and prev_lon is not None:
                    distance = haversine((prev_lat, prev_lon), (lat, lon), unit=Unit.KILOMETERS)
                    total_distance += distance

                prev_lat = lat
                prev_lon = lon

        print(f"Total distance walked by user 112 in 2008: {round(total_distance,2)} km")
        return total_distance

    def get_most_used_transportation_mode(self):
            print("Finding users with their most used transportation mode...")

            pipeline = [
                {"$match": {"transportation_mode": {"$ne": None}}},  
                {
                    "$group": {
                        "_id": {
                            "user_id": "$user_id",
                            "transportation_mode": "$transportation_mode"
                        },
                        "mode_count": {"$sum": 1} 
                    }
                },
                {
                    "$sort": {
                        "_id.user_id": 1,  
                        "mode_count": -1    
                    }
                }
            ]

            result = self.db['Activity'].aggregate(pipeline)
            
            most_used_modes = defaultdict(lambda: (None, 0))  

            for doc in result:
                user_id = doc['_id']['user_id']
                transportation_mode = doc['_id']['transportation_mode']
                mode_count = doc['mode_count']
                
                if mode_count > most_used_modes[user_id][1]:
                    most_used_modes[user_id] = (transportation_mode, mode_count)
            
            if most_used_modes:
                table_data = [[user_id, mode] for user_id, (mode, _) in sorted(most_used_modes.items())]
                table = tabulate(table_data, headers=["User ID", "Most Used Transportation Mode"], tablefmt="pretty")
                print("\nMost used transportation modes per user:")
                print(table)
            else:
                print("No transportation mode data available.")

    def top_20_users_by_altitude_gain(self):
        user_altitude_gain = {}

        activities = self.db['Activity'].find({'trackpoint_ids': {'$exists': True, '$not': {'$size': 0}}})

        for activity in tqdm(activities, desc="Processing activities", unit="activity"):
            user_id = activity['user_id']
            trackpoint_ids = activity['trackpoint_ids']

            trackpoints = list(self.db['TrackPoint'].find({'_id': {'$in': trackpoint_ids}}).sort('date_time', 1))

            for i in range(1, len(trackpoints)):
                current_altitude = trackpoints[i]['altitude']
                previous_altitude = trackpoints[i - 1]['altitude']

                if current_altitude != -777 and current_altitude > previous_altitude:
                    altitude_gain = (current_altitude - previous_altitude) * 0.3048  
                    if user_id in user_altitude_gain:
                        user_altitude_gain[user_id] += altitude_gain
                    else:
                        user_altitude_gain[user_id] = altitude_gain

        top_20_users = sorted(user_altitude_gain.items(), key=lambda x: x[1], reverse=True)[:20]

        if top_20_users:
            print("Top 20 Users by Altitude Gain (User ID, Total Meters Gained):")
            print(tabulate(top_20_users, headers=["User ID", "Total Meters Gained"], tablefmt="grid"))
        else:
            print("No altitude data found.")


    def find_users_with_invalid_activities(self):
        invalid_activities_per_user = {}

        activities = self.db['Activity'].find({'trackpoint_ids': {'$exists': True, '$not': {'$size': 0}}})

        for activity in activities:
            user_id = activity['user_id']
            activity_id = activity['_id']
            trackpoint_ids = activity['trackpoint_ids']

            trackpoints = list(self.db['TrackPoint'].find({'_id': {'$in': trackpoint_ids}}).sort('date_time', 1))

            previous_trackpoint_time = None
            invalid_activity_found = False

            for trackpoint in trackpoints:
                trackpoint_time = trackpoint['date_time']

                if previous_trackpoint_time:
                    time_diff = (trackpoint_time - previous_trackpoint_time).total_seconds() / 60.0

                    if time_diff >= 5:
                        invalid_activity_found = True  

                previous_trackpoint_time = trackpoint_time

            if invalid_activity_found:
                if user_id not in invalid_activities_per_user:
                    invalid_activities_per_user[user_id] = 0
                invalid_activities_per_user[user_id] += 1

        if invalid_activities_per_user:
            table_data = [[user_id, count] for user_id, count in sorted(invalid_activities_per_user.items(), key=lambda x: x[1], reverse=True)]
            table = tabulate(table_data, headers=["User ID", "Invalid Activity Count"], tablefmt="pretty")
            print("Users with Invalid Activities:")
            print(table)
        else:
            print("No users with invalid activities found.")
            
    def find_users_in_forbidden_city(self):
        forbidden_city_lat = 39.916
        forbidden_city_lon = 116.397
        tolerance = 0.001  

        lat_min = forbidden_city_lat
        lat_max = forbidden_city_lat + tolerance
        lon_min = forbidden_city_lon
        lon_max = forbidden_city_lon + tolerance

        matching_trackpoints = self.db['TrackPoint'].find({
            "lat": {"$gte": lat_min, "$lte": lat_max},
            "lon": {"$gte": lon_min, "$lte": lon_max}
        }, {"_id": 1})  

        trackpoint_ids = [tp['_id'] for tp in matching_trackpoints]

        if not trackpoint_ids:
            print("No trackpoints found within the Forbidden City area.")
            return

        activities_with_matching_trackpoints = self.db['Activity'].find({
            "trackpoint_ids": {"$in": trackpoint_ids}
        }, {"user_id": 1})  

        user_ids = {activity['user_id'] for activity in activities_with_matching_trackpoints}

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
        
        program.show_collections()
        user_count, activity_count, trackpoint_count = program.count_users_activities_trackpoints()
        print(f"User Count: {user_count}, Activity Count: {activity_count}, TrackPoint Count: {trackpoint_count}")
        average = program.avg_activities_per_user()
        print(f"Average activities per user: {average}")
        program.top_20_users_with_most_activities()
        program.find_taxi_users()
        program.count_transportation_modes()
        program.find_year_with_most_activities_and_hours()
        program.distance_walked()
        program.top_20_users_by_altitude_gain()
        program.find_users_with_invalid_activities()
        program.find_users_in_forbidden_city()
        program.get_most_used_transportation_mode()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
