from pprint import pprint 
from DbConnector import DbConnector
import os
from datetime import datetime

class Task_1_Program:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)


    def insert_users(self, base_dir, labeled_ids_file):
        with open(labeled_ids_file, 'r') as f:
            labeled_ids = {line.strip() for line in f}
        
        user_docs = []
        
        for user_folder in os.listdir(base_dir):
            user_path = os.path.join(base_dir, user_folder)
            
            if os.path.isdir(user_path):
                user_id = user_folder
                
                # Check if the user has labeled transportation
                is_labeled = user_id in labeled_ids
                
                user_docs.append({
                    "_id": user_id,  # Use user_id as the _id field
                    "is_labeled": is_labeled
                })
        
        if user_docs:
            self.db['User'].insert_many(user_docs)
            print(f"Inserted {len(user_docs)} users.")


    def insert_activities(self, base_dir):
        for root, dirs, files in os.walk(base_dir):
            if 'Trajectory' in dirs:
                user_id = os.path.basename(root)
                labels = {}
                labels_file_path = os.path.join(root, 'labels.txt')
                
                if os.path.exists(labels_file_path):
                    with open(labels_file_path, 'r') as f:
                        f.readline()  # Skip header line
                        for line in f:
                            start_time_str, end_time_str, transportation_mode = line.strip().split('\t')
                            start_time = datetime.strptime(start_time_str, "%Y/%m/%d %H:%M:%S")
                            end_time = datetime.strptime(end_time_str, "%Y/%m/%d %H:%M:%S")
                            labels[(start_time, end_time)] = transportation_mode

                trajectory_folder = os.path.join(root, 'Trajectory')
                activity_docs = []

                for plt_file in os.listdir(trajectory_folder):
                    if plt_file.endswith(".plt"):
                        file_path = os.path.join(trajectory_folder, plt_file)
                        with open(file_path, 'r') as file:
                            lines = file.readlines()
                            line_count = len(lines)

                            if line_count - 6 <= 2500:
                                start_time = lines[6].strip().split(',')[6]
                                start_date = lines[6].strip().split(',')[5]
                                end_time = lines[-1].strip().split(',')[6]
                                end_date = lines[-1].strip().split(',')[5]
                                start_date_time = start_date + ' ' + start_time
                                end_date_time = end_date + ' ' + end_time
                                start_date_time = datetime.strptime(start_date_time, "%Y-%m-%d %H:%M:%S")
                                end_date_time = datetime.strptime(end_date_time, "%Y-%m-%d %H:%M:%S")

                                transportation_mode = None
                                for (label_start, label_end), mode in labels.items():
                                    if label_start == start_date_time and label_end == end_date_time:
                                        transportation_mode = mode
                                        break
                                
                                activity_docs.append({
                                    "_id": int(plt_file.split('.')[0] + user_id),  # Unique ID for activity
                                    "user_id": user_id,
                                    "transportation_mode": transportation_mode,
                                    "start_date_time": start_date_time,
                                    "end_date_time": end_date_time,
                                    "trackpoint_ids": []  # Initialize as an empty list
                                })
                
                if activity_docs:
                    self.db['Activity'].insert_many(activity_docs)
                    print(f"Inserted {len(activity_docs)} activities for user {user_id}.")



    def insert_trackpoints(self, base_dir):
        batch_size = 10000  # Number of trackpoints to insert in each batch
        file_count = 0  # Counter to track number of files processed

        for root, dirs, files in os.walk(base_dir):
            if 'Trajectory' in dirs:
                user_id = os.path.basename(root)
                trajectory_folder = os.path.join(root, 'Trajectory')

                for file_name in os.listdir(trajectory_folder):
                    file_path = os.path.join(trajectory_folder, file_name)
                    activity_id = int(file_name.split('.')[0] + user_id)

                    with open(file_path, 'r') as file:
                        lines = file.readlines()
                        line_count = len(lines)

                        if line_count - 6 <= 2500:  # Process only files with <= 2500 data lines
                            print(f"Processing file: {file_name} for user: {user_id}")
                            trackpoint_docs = []  # Trackpoints batch for this file
                            trackpoint_ids = []  # List to store inserted trackpoint IDs

                            for line in lines[6:]:
                                data = line.strip().split(',')
                                latitude = float(data[0])
                                longitude = float(data[1])
                                altitude = float(data[3])
                                date_days = float(data[4])
                                date_time_str = data[5] + ' ' + data[6]
                                date_time = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")

                                # Prepare the trackpoint document
                                trackpoint_doc = {
                                    "activity_id": activity_id,
                                    "lat": latitude,
                                    "lon": longitude,
                                    "altitude": altitude,
                                    "date_days": date_days,
                                    "date_time": date_time
                                }
                                trackpoint_docs.append(trackpoint_doc)

                                # If batch size is reached, insert the batch
                                if len(trackpoint_docs) >= batch_size:
                                    result = self.db['TrackPoint'].insert_many(trackpoint_docs)
                                    trackpoint_ids.extend(result.inserted_ids)  # Collect the inserted IDs
                                    print(f"Inserting batch of {len(trackpoint_docs)} trackpoints...")
                                    trackpoint_docs.clear()  # Clear the batch after insertion

                            # Insert any remaining trackpoints after file processing
                            if trackpoint_docs:
                                result = self.db['TrackPoint'].insert_many(trackpoint_docs)
                                trackpoint_ids.extend(result.inserted_ids)
                                print(f"Inserting final batch of {len(trackpoint_docs)} trackpoints...")

                            # Update the activity document with the list of trackpoint_ids
                            self.db['Activity'].update_one(
                                {"_id": activity_id},
                                {"$push": {"trackpoint_ids": {"$each": trackpoint_ids}}}  # Push all trackpoint_ids at once
                            )

                            file_count += 1

        print(f"Finished processing {file_count} files.")




    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            print(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['group13_ex3'].list_collection_names()
        print(collections)

    def list_all_users(self):
        users = self.db['User'].find({})
        for user in users:
            print(user)

    def empty_collection(self, collectionName):
        self.db[collectionName].delete_many({}) # Sett inn navn på database du ønsker å tømme
        print(f"All {collectionName} have been deleted from the collection.")

def main():
    program = None
    try:
        program = Task_1_Program()
        #program.create_coll('User')
        #program.create_coll('Activity')
        #program.create_coll('TrackPoint')
        program.show_coll()
        #program.insert_users(base_dir="dataset/dataset/Data", labeled_ids_file="dataset/dataset/labeled_ids.txt")
        #program.insert_activities(base_dir="dataset/dataset/Data")
        #program.insert_trackpoints(base_dir="dataset/dataset/Data")
        #program.empty_collection('Activity')
        #program.list_all_users()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
