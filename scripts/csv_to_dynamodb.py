import csv
import boto3

def convert_csv_to_json_list(file):
   items = []
   with open(file) as csvfile:
      reader = csv.DictReader(csvfile)
      for row in reader:
        data = {}
        data['Name'] = row['Name']
        data['Sex'] = row['Sex']
        data['Age'] = row['Age']
        data['Height(in)'] = row['Height(in)']
        data['Weight(lbs)'] = row['Weight(lbs)']
        print(row)
          #populate remaining fields here
          #................
        items.append(data)
   return items

def batch_write(items):

   dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000', region_name="localhost")
   db = dynamodb.Table('Biometrics')

   with db.batch_writer() as batch:
      for item in items:
         batch.put_item(Item=item)

if __name__ == '__main__':
   json_data = convert_csv_to_json_list('biostats.csv')
   batch_write(json_data)