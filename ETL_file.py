import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(filename):
    dataframe = pd.read_csv(filename)

    return dataframe

def extract_from_json(filename):
    dataframe = pd.read_json(filename, lines=True)

    return dataframe

def extract_from_xml(filename):
    # create an empty data frame to be populated
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(filename)
    root = tree.getroot()

    # run through items in root and assigned values to local variables
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text

        # add variables values to empty data frame
        dataframe = pd.concat([dataframe, pd.DataFrame([{"car_model" : car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}])], ignore_index=True)

    return dataframe

def extract():
    # create empty data frame to be populated
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])

    # process all csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    for xml_file in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xml_file))], ignore_index=True)

    return extracted_data

def transform(data):
    data["price"] = round(data.price, 2)

    for i in range(len(data["fuel"])):
        if data["fuel"][i] == "Petrol":
            data["fuel"][i] = "Gasoline"

    return data

def load_data(t_file, transformed_data):
    transformed_data.to_csv(t_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'

    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as file:
        file.write(timestamp + ", " + message + "\n")


def runcode():
    # Log the initialization of the ETL process
    log_progress("ETL Job Started")

    # Log the beginning of the Extraction process
    log_progress("Extract phase Started")
    extracted_data = extract()

    # Log the completion of the Extraction process
    log_progress("Extract phase Ended")

    # Log the beginning of the Transformation process
    log_progress("Transform phase Started")
    transformed_data = transform(extracted_data)
    print("Transformed Data")
    print(transformed_data)

    # Log the completion of the Transformation process
    log_progress("Transform phase Ended")

    # Log the beginning of the Loading process
    log_progress("Load phase Started")
    load_data(target_file, transformed_data)

    # Log the completion of the Loading process
    log_progress("Load phase Ended")

    # Log the completion of the ETL process
    log_progress("ETL Job Ended")

runcode()


