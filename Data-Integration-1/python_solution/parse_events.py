import json
import csv
import os.path
import datetime
import argparse


class json_reporter:
    '''
    Converts Logging Data in JSON into CSV format for analysis
    '''
    def __init__(self, input_file, output_file):

        self.input_file = input_file
        self.output_file = output_file

        # Statistic Numbers for report
        # Method needs to access an instance variable filtering
        self.dropped_events = {'No action mapping': 0, 'Duplicate': 0}

        try:
            with open(self.input_file, 'r') as jsonFile:
                # Sample file was not a valid JSON format
                # i.e List of JSON Objects
                # Each line was a valid JSON though.
                # try:
                #     self.raw_data = json.load(jsonFile)
                # except json.decoder.JSONDecodeError:
                #     raise Exception('Possibly Corrupted JSON file')

                # Note: Each line is already a dictionary
                self.raw_data = []
                test = jsonFile.readlines()
                for item in test:
                    try:
                        self.raw_data.append(json.loads(item))
                    except ValueError:
                        # Continue if there is just one corrupted item
                        print ('Unable to read json item')
                        pass

            jsonFile.close()
        except IOError:
                raise Exception('Unable to open file')

    def filter_list_of_dict(self, list_of_dict):
        '''
        This takes a list of dictionary from the parsed JSON file
        It will filter(drop) items that are duplicate or where keys
        do not map to the correct activity
        '''
        event_id_tracked = []
        mapped_activities = {'ADD': '', 'REMOVE': '', 'ACCESSED': ''}
        mapped_activities['ADD'] = ['createdDoc', 'addedText', 'changedText']
        mapped_activities['REMOVE'] = ['deletedDoc', 'deletedText', 'archived']
        # Note: On Guide, we look for viewDoc. Corrected it Here
        mapped_activities['ACCESSED'] = ['viewedDoc']
        filtered_list_of_dict = []

        for dict_item in list_of_dict:
            add_item = True
            if dict_item['eventId'] not in event_id_tracked:
                # Classify activity
                if dict_item['activity'] is not None:
                    if dict_item['activity'] in mapped_activities['ADD']:
                        dict_item['activity'] = 'ADD'
                    elif dict_item['activity'] in mapped_activities['REMOVE']:
                        dict_item['activity'] = 'REMOVE'
                    elif dict_item['activity'] in mapped_activities['ACCESSED']:
                        dict_item['activity'] = 'ACCESSED'
                    else:
                        self.dropped_events['No action mapping'] += 1
                        add_item = False
            else:
                self.dropped_events['Duplicate'] += 1
                add_item = False

            if add_item:
                filtered_list_of_dict.append(dict_item)
                event_id_tracked.append(dict_item['eventId'])

        return filtered_list_of_dict

    def __convert_to_new_dict(self, dict_item):
        '''
            Converts dictionary item (raw data) from Parsed JSON file
            to the new format as required by the CSV file output
            It transforms they appropriate keys for the CSV field headers
            and also deals with converting the time stamp
        '''
        new_dict_item = {}
        # Check for the timeoffset, if not present add it
        keys = dict_item.keys()
        if 'timeOffset' not in keys:
            dict_item['timeOffset'] = '+00:00'

        for key, value in dict_item.items():

            if key == 'timestamp':

                try:
                    dt = datetime.datetime
                    curdate = dt.strptime(dict_item[key],
                                          '%m/%d/%Y %I:%M:%S%p')
                    new_dict_item['Timestamp'] = curdate.isoformat()

                except:
                    raise Exception('Invalid Time Stamp Found')
                    curdate = None
                    new_dict_item['Timestamp'] = None
                    pass

            elif key == 'timeOffset':
                        offset = dict_item[key]
                        new_dict_item['Timestamp'] += offset

            elif key == 'activity':
                new_dict_item['Action'] = dict_item[key]
            elif key == 'user':
                new_dict_item['User'] = dict_item[key]
            elif key == 'file':
                filename_val = os.path.basename(value)
                directory_val = os.path.dirname(value)
                new_dict_item['Folder'] = directory_val
                new_dict_item['File Name'] = filename_val

            elif key == 'ipAddr':
                new_dict_item['IP'] = dict_item['ipAddr']

        return new_dict_item

    def build_new_list_of_dict(self, list_of_dict):
        '''
        Converts a list of dictionary from the parsed JSON file
        to the new format as required by the new CSV header fields
        '''
        new_list_of_dict = []

        for item in list_of_dict:
            new_item = self.__convert_to_new_dict(item)
            new_list_of_dict.append(new_item)

        return new_list_of_dict

    def write_file(self, list_of_dict):
        '''
        Generate CSV Output file from dictionary.
        '''

        try:
            with open(self.output_file, 'w+',
                      newline='') as csvFile:
                fields = ['Timestamp', 'Action', 'User', 'Folder',
                          'File Name', 'IP']
                writer = csv.DictWriter(csvFile,
                                        fieldnames=fields, lineterminator='\n',
                                        quoting=csv.QUOTE_ALL)
                writer.writeheader()

                for item in list_of_dict:
                    writer.writerow(item)

            csvFile.close()
        except IOError:
                print('File Writing Problem')

    def get_stats(self, list_of_dict):
        '''
        list_of_dict hast the following keys
        fields = ['Timestamp', 'Action', 'User', 'Folder',
                  'File Name', 'IP']
        '''
        stats = {
            "linesRead": 0,
            "droppedEventsCounts": 0,
            "droppedEvents": {
                "No action mapping": 0,
                "Duplicate": 0
            },
            "uniqueUsers": 0,
            "uniqueFiles": 0,
            "startDate": 0,
            "endDate": 0,
            "actions": {
                "ADD": 0,
                "REMOVE": 0,
                "ACCESSED": 0
            }
        }

        print('Here are some stats!')
        stats['linesRead'] = len(self.raw_data)
        stats['droppedEventsCounts'] = sum(self.dropped_events.values())
        stats['droppedEvents']['No action mapping'] \
            = self.dropped_events['No action mapping']
        stats['droppedEvents']['Duplicate'] = self.dropped_events['Duplicate']
        stats['uniqueUsers'] = len(set([i['User']
                                   for i in list_of_dict]))
        # Unique files must be unique path
        file_tuple = [(i['Folder'], i['File Name'])
                      for i in list_of_dict]
        file_abspath_list = [folder + filename
                             for (folder, filename) in file_tuple]
        stats['uniqueFiles'] = len(set(file_abspath_list))
        stats['actions']['ADD'] = len([i for i in
                                      list_of_dict if i['Action'] == 'ADD'])
        stats['actions']['REMOVE'] = len([
                        i for i in list_of_dict if i['Action'] == 'REMOVE'])
        stats['actions']['ACCESSED'] = len([
                        i for i in list_of_dict if i['Action'] == 'ACCESSED'])

        # Dates
        # Magic Parsing
        # Extract the timestamps, on list, remove ':' on offset
        # So we can use it for python3 datetime
        dates_list = [''.join(i['Timestamp'].rsplit(':', 1))
                      for i in list_of_dict]
        dt = datetime.datetime
        # Convert to Python Datetime
        dates_list = [dt.strptime(date, '%Y-%m-%dT%H:%M:%S%z')
                      for date in dates_list]
        stats['startDate'] = min(dates_list).isoformat()
        stats['endDate'] = max(dates_list).isoformat()

        print (json.dumps(stats, sort_keys=False,
                          indent=4, separators=(',', ': ')))


if __name__ == '__main__':

        parser = argparse.ArgumentParser()
        parser.add_argument("input_json",
                            help="Input filename path in JSON format")
        parser.add_argument("output_csv", help="Output filename in CSV format")
        args = parser.parse_args()
        json_reporter = json_reporter(args.input_json, args.output_csv)
        filtered_data = json_reporter\
            .filter_list_of_dict(json_reporter.raw_data)
        new_list_of_dict = json_reporter.build_new_list_of_dict(filtered_data)
        json_reporter.write_file(new_list_of_dict)
        json_reporter.get_stats(new_list_of_dict)
