# Data Integration, Task 1

## Scenario:

A customer has an online, cloud document editing program. Their software records all of the changes that have occurred to the online documents. They would like us to analyse the changes' metadata to identify user behaviors, and have given us a small sample file containing approximately 10,000 lines of JSON objects representing metadata for this purpose.

Your task is to write a Java 1.8 program, built with Maven, to;

1. read and parse the JSON file
2. produce a filtered CSV file for our behaviour analytics team to analyze, and
3. aggregate and print out statistics at the end.

You may use any open source, public libraries to help you process the metadata. We recommend Jackson for JSON, Commons CSV for CSV, and Joda or Java DateTime for working with timestamps.

Your program should;

1. be runnable from a command prompt,
2. take two arguments: one for a JSON file to read and one for a CSV file to write, and
3. after completion, print out the statistics to the console.

Do not tie your program to any one specific operating system, it should be able to run on various operating systems. Keep the solution purely Java. If you feel the need to deviate from Java, explain your reasoning.

Feel free to fork this GitHub repo to do your work. When you feel that you are done, email us a link to your fork, and we'll review.

[The JSON Metadata](https://github.com/rwallinterset/Interview/blob/master/Data-Integration-1/README.MD#the-json-metadata) section will outline what you should expect from the metadata file we’ve provided to you.

[The CSV File](https://github.com/rwallinterset/Interview/blob/master/Data-Integration-1/README.MD#the-csv-file) section will cover the output format we expect.

[The Metrics Output](https://github.com/rwallinterset/Interview/blob/master/Data-Integration-1/README.MD#the-metrics-output) section will detail what we expect printed out to the console after the program has completed.

The [Additional Assumptions You Can Make](https://github.com/rwallinterset/Interview/blob/master/Data-Integration-1/README.MD#additional-assumptions-you-can-make) section lists some general things to keep in mind as you work on this task.

## The JSON Metadata:

The metadata file has been included with this task. Each line is a single JSON object with the following key/values:

|Key|Data Type|Description|Examples|
|---|---|---|---|
|eventId|Long|Unique ID for this metadata object.|43985498|
|user|String|The user performing the activity. Either “*@customer.com”, “SYSTEM”, or “ADMIN”.|jsmith@customer.com, SYSTEM, ADMIN
|ipAddr|String|The IP address of the user.|10.10.12.25|
|file|String|Fully qualified, unix style path name for the file being acted on.|/data/onlineDocs/2015/Q4/expenses-jan.doc|
|activity|String|The activity that the user is performing on the file. All values are below this chart.|viewedDoc|
|timestamp|String|Date in the format “MM/DD/YY HH:MM:SS” followed by either “am” or “pm”.|11/04/2015 01:15:37PM|
|timeOffset|String|Current timezone offset in the format “-##:##’. Field may not be present. If missing, assume UTC timezone.|-05:00, -08:00, +08:30|

Activity can be one of:
- createdDoc
- deletedDoc
- viewedDoc
- addedText
- changedText
- deletedText
- hashed
- replicated
- archived
- restored

Sample:
```
{
    "eventId": 43985498,
    "user": "jsmith@customer.com",
    "ipAddr": "10.10.12.35",
    "file": "/data/onlineDocs/2015/Q4/expenses-Jan.doc",
    "activity": "viewedDoc",
    "timestamp": "11/04/2015 1:15:37pm",
    "timeOffset": "-05:00"
}
```
**Note:** The sample above has been written on multiple lines for readability; in the metadata file provided, each JSON object will be on a single line.

## The CSV File ##

The output CSV file should be comma delimited, fields should be quoted if necessary, and formatted to this schema;

`TIMESTAMP,ACTION,USER,FOLDER,FILENAME,IP`

|Column|Description|Examples|
|---|---|---|---|
|Timestamp|ISO 8601 compliant String, with milliseconds and timezone.|2015-11-04T13:15:37.000-05:00, 2015-11-05T14:45:34.000Z|
|Action|Our interpretation of the activities, one of “ADD”, “REMOVE” or “ACCESSED”. Mapping below.|ACCESSED|
|User|The user performing the action, without “@customer.com”|jsmith|
|Folder|The path to the file.|/data/onlineDocs/2015/Q4/|
|File Name|The file in above folder.|expenses-Jan.doc|
|IP|The IP address related to the activity.|10.10.12.35|

Action Mappings:

|CSV Action|Metadata Activity|
|---|---|
|ADD|createdDoc, addedText, changedText|
|REMOVE|deletedDoc, deletedText, archived|
|ACCESSED|viewDoc|

Any valid metadata objects that fit at least one of the following criteria should be dropped, and excluded from the CSV file results.
- Any metadata objects with activity that is not mapped above.
- Duplicate metadata objects, indicated by the event ID.
  - You only have to check the eventId. You do not have to compare the entire event.

## The Metrics Output ##

You should output a single JSON object (preferably ‘pretty’ formatted) that has the following statistics and structure.

- How many lines were read
- How many were dropped 
- Counts for why a line was dropped
- How many unique users 
- How many unique files
- The time range of the data set
- Counts of each action encountered

Sample output:
```
{
    "linesRead":10000,
    "droppedEventsCounts": 150
    "droppedEvents": {
        "No action mapping": 134,
        "Duplicates": 16
    },
    "uniqueUsers": 23,
    "uniqueFiles": 256,
    "startDate": "2015-11-04T13:15:37.000-05:00",
    "endDate": "2016-01-15T06:54:23.000-05:00",
    "actions": {
        "ADD": 5550,
        "REMOVE": 2025,
        "ACCESSED": 2275
    }
}
```
**Notes:**

Other than “lines read” and “event dropped”, dropped messages should not affect the other statistics.

The sample above does not contain the values we expect for the given data set, There may not be 23 users, or 256 files.

## Additional Assumptions You Can Make ##

- Each line will always be valid JSON
- Key:values in the metadata objects will always be present unless [The JSON Metadata](https://github.com/rwallinterset/Interview/blob/master/Data-Integration-1/README.MD#the-json-metadata) section indicates otherwise.
- Value type (Int, String) will always match the type listed in [The JSON Metadata](https://github.com/rwallinterset/Interview/blob/master/Data-Integration-1/README.MD#the-json-metadata) section.
- If you encounter a metadata object you can't parse for reasons not covered in [The CSV File](https://github.com/rwallinterset/Interview/blob/master/Data-Integration-1/README.MD#the-csv-file), do not include it in the CSV output.
  - Print out an error, with a simple explanation
