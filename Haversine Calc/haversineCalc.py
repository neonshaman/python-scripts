from haversine import haversine, Unit
import csv
import ctypes as ct

# If zip value is fewer than 5 characters, append a string of zeros to left
# side until a complete zip code is created, then return it
def validate_zip(unvalidated_zip):
    if len(str(unvalidated_zip)) < 5:
        while len(str(unvalidated_zip)) < 5:
            unvalidated_zip = "0" + unvalidated_zip
    return unvalidated_zip

# Main procedure
if __name__ == '__main__':
    input_zip = input('Please enter a target zip code: ')
    filepath = input('Please provide an absolute path to your input csv: ')
    filename = input('Please provide a filename and extension for your output csv: ')
    input_coords = (0.0 ,0.0)
    compare_coords = (0.0, 0.0)
    # Increase CSV buffer size to read in entire CSV
    csv.field_size_limit(int(ct.c_ulong(-1).value // 2))
    # Input file
    with open(filepath, 'r') as csv_input, open(filename, 'a', newline='') as csv_output:
        print("Input Zip: " + input_zip)
        r = csv.reader(csv_input, delimiter = ',')
        # Search reader data for target zip coordinates 
        for row in r:
            validated_zip = validate_zip(row[0])
            if validated_zip == input_zip:
                input_coords = (float(row[1]), float(row[2]))            # lat/long
        print(input_coords)
        w = csv.writer(csv_output)
        w.writerow(["target_zipcode", "nearby_zipcode", "distance"])
        csv_input.seek(0)
        next(r)
        for row in r:
            validated_zip = validate_zip(row[0])
            compare_coords = (float(row[1]), float(row[2]))              # lat/long
            # Haversine fomula reads great circle distance between two points on
            # a sphere given their lat/long unless points are antipodal
            distance = haversine(input_coords, compare_coords, unit= Unit.MILES)
            print("Haversine reached")
            print(distance)
            if distance < 200:
                # Write to output csv
                w.writerow([input_zip, validated_zip, distance])