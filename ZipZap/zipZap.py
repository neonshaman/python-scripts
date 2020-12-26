from geomet import wkb
import json
import os
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
import uuid
from shapely.geometry import shape, MultiPolygon, Polygon, Point
import codecs
 
# On/Off toggle for Mongo client, return client handle on open 
def mongo_client_toggle(toggle, db):
    if toggle == "open":
        # mongo_prod = os.environ.get("MONGO_PROD")
        # client = MongoClient(mongo_prod)
        # return client
        
        mongo_dev = os.environ.get("MONGO_DEV")
        client = MongoClient(mongo_dev)
        return client
    else:
        db.close()
        print("\nMongo client closed.")
        
# Check Mongo for presence of zip code, return boolean
def mongo_find_zip(zip_code, db):
    if db.geo_zips.find_one({"zip": zip_code}) == None:
        return False
    else:
        return True
    
# Generate and return an _id Mongo field, each value is a 32 char hexadecimal
# representation. UUID4 algorithm selected because MAC address cannot be 
# extracted from it. Difficult to exploit, but still best security practice.
def generate_id():
    id = uuid.uuid4().hex
    return id

# Take a shapely geometry object, dump it into a wkb, then convert from default
# hexadecimal to base 64 representation and return it
def get_wkb_64(geom):
    wkb_hex = geom.wkb_hex
    wkb_64 = codecs.encode(codecs.
                            decode(str(wkb_hex),
                                    'hex'), 'base64').decode()
    return wkb_64
        
# Load data and write to Mongo
def update_geozips_in_mongo(filepath, db):
    with open(filepath, "r") as geo_file:
        # Read in data beginning with second line to skip FeatureCollection
        # formatting
        file_data = json.load(geo_file)
        bulk_write_requests = []
        insert_list= []
        for feature in file_data["features"]:
            zip_code = feature["properties"]["ZIP_CODE"]
            # Zip already exists in Mongo, needs update
            if mongo_find_zip(zip_code, db) == True:
                city_name = feature["properties"]["PO_NAME"]
                area = feature["properties"]["SQMI"]
                poly_coords = feature["geometry"]["coordinates"]
                # Data is mixed geometry types, convert Polygons to MultiPoly
                poly_type = "MultiPolygon"
                if feature["geometry"]["type"] == "Polygon":
                    mod_coords = []
                    mod_coords.insert(0, poly_coords)
                    poly_coords = mod_coords
                # Adapt context to geometry interface for shapely functions
                data = {"type": poly_type, "coordinates": poly_coords}
                geom = shape(data)
                # Get centroid coords and format them from Point object
                centroid = geom.centroid
                centroid_x = centroid.x
                centroid_y = centroid.y
                centroid_coords = [centroid_x, centroid_y]
                # WKB in base 64 representation
                wkb = get_wkb_64(geom)
                # Add request to update list for bulk write
                bulk_write_requests.append(
                    UpdateOne({"zip":zip_code}, 
                              {"$set":
                        {
                            "city_name" : city_name,
                            "features.0.properties.wkb" : wkb,
                            "features.0.properties.area": area,
                            "features.0.geometry.type": poly_type,
                            "features.0.geometry.coordinates": poly_coords,
                            "loc.type" : "Point",
                            "loc.coordinates": centroid_coords
                        }}
                              ))
            # Record does not yet exist in Mongo, needs insert
            else:
                city_name = feature["properties"]["PO_NAME"]
                area = feature["properties"]["SQMI"]
                poly_coords = feature["geometry"]["coordinates"]
                state = feature["properties"]["STATE"]
                mong_id = generate_id()
                # Data is mixed geometry types, convert Polygons to MultiPoly
                poly_type = "MultiPolygon"
                if feature["geometry"]["type"] == "Polygon":
                    mod_coords = []
                    mod_coords.insert(0, poly_coords)
                    poly_coords = mod_coords
                # Adapt context to geometry interface for shapely functions
                data = {"type": poly_type, "coordinates": poly_coords}
                geom = shape(data)
                # Get centroid coords and format them from Point object
                centroid = geom.centroid
                centroid_x = centroid.x
                centroid_y = centroid.y
                centroid_coords = [centroid_x, centroid_y]
                # WKB in base 64 representation
                wkb = get_wkb_64(geom)
                # Add request to update list for bulk write
                bulk_write_requests.append(
                    InsertOne(
                        {
                        "_id" : mong_id,
                        "zip" : zip_code,
                        "state" : state,
                        "city_name" : city_name,
                        "features" : [ 
                            {
                                "type" : "Feature",
                                "properties" : {
                                    "wkb" : wkb,
                                    "area" : area,
                                                },
                                "geometry" : {
                                    "type" : poly_type,
                                    "coordinates" : poly_coords
                                             },
                            },
                                     ],
                        "loc" : {
                            "type" : "Point",
                            "coordinates" : centroid_coords,
                                }
                        }
                                )
                                            )
                insert_list.append(zip_code)
        # Unordered bulk write orders of magnitude more efficient in execution
        try:        
            res = db.geo_zips.bulk_write(bulk_write_requests, ordered=False)
            print(res.bulk_api_result)               
            print("Total # of Requests: " + str(len(bulk_write_requests)))
        except BulkWriteError as bwe:
            print(bwe.details)
            
# Main Procedure
if __name__ == "__main__":
    db = mongo_client_toggle("open", "none")
    filepath = "/home/jacobalo/Desktop/zip_poly.json"   # ENTER YOUR PATH
    update_geozips_in_mongo(filepath, db.dev)           # DEV TEST
    # update_geozips_in_mongo(filepath, db.prod)          # PROD
    mongo_client_toggle("close", db)