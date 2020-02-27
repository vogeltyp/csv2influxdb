#!/usr/bin/env python3
# SCRIPT:       csv2influxdb
# AUTHOR:       Holger (holger@sohonet.ch)
# DATE:         20200227
# REVISION:     1.1
# PLATFORM:     Python
# PURPOSE:      parsing CSV-Files to InfluxDB
# REV. LIST:    DATE            AUTHOR      MODIFICATION
# 1.1           20200227        Holger      - added userauth for InfluxDB
#                                           - added comments
# 1.0           20200216        Holger      - initial script


import pytz
import sys
import argparse
from influxdb import InfluxDBClient
from datetime import datetime


# global variables
runCounter = 0
shownPercentage = -1
client = InfluxDBClient()
tscols = ""
args = ""


def usage():
    global args
    # define commandline switches and print them out if needed.
    parser = argparse.ArgumentParser(prog='csv2influxDB', description='Script to move CSV-Data to an InfluxDB', formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument('-i', '--input', metavar='FILENAME', action='store', help='CSV-File to read\n ', required=True)
    parser.add_argument('-h', '--host', metavar='INFLUXDB_HOST[:PORT]', action='store', help='Target host[:port] with InfluxDB\n ', required=True)
    parser.add_argument('-d', '--database', metavar='INFLUXDB_DB', action='store', help='Target database\n ', required=True)
    parser.add_argument('-u', '--username', metavar='USERNAME', action='store', help='Database user\n ', required=True)
    parser.add_argument('-p', '--password', metavar='PASSWORD', action='store', help='Password for database user\n ', required=True)
    parser.add_argument('-m', '--measurement', metavar='MEASUREMENT', action='store', help='Target measurement at given database\n ', required=True)
    parser.add_argument('-t', '--tags', metavar='TAGKEY=TAGVALUE[,TAGKEY=TAGVALUE]', action='store', help='Tagkey=Tagvalue,Tagkey=Value etc. for entries\n ')
    parser.add_argument('-f', '--fields', metavar='FILEDKEY[,FILEDKEY]', action='store', help='Field,Field etc. for columns\n ', required=True)
    parser.add_argument('-ts', '--timestamp', metavar='%m/%d/%Y,%H:%M', action='store', help='define timestamp\nExpl: %%m/%%d/%%Y,%%H:%%M means\n'
                                                                                             '2 columns. comma seperated, 1st with date and 2nd with time\n'
                                                                                             '01/17/2020,03:03,....\n ', required=True)
    parser.add_argument('-tz', '--timezone', metavar='Europe/Zurich', action='store', help='set timezone of CSV-Values\n ', default='UTC')
    parser.add_argument('-tzh', '--timezonehelp', action='store_true', help='show all possible timezones\n ')
    parser.add_argument('-b', '--bulk', action='store_true', help='do a bulkimport, not line by line\nAttention! Values will be overwritten!!!', default=False)

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        print("\n\n")
        sys.exit(0)

    args = parser.parse_args()


def calcPercent(maxVal, actVal):
    _val = 100 * actVal / maxVal
    return _val


def createTimestamp(data):
    global tscols, args
    _timeformat = str(args.timestamp)
    tscols = len(args.timestamp.split(","))
    _data = ""
    for col in range(0, tscols):
        _data += data[col]
        if col < (tscols - 1):
            _data += ","
    try:
        setAsTimezone = pytz.timezone(args.timezone)
        _timestamp = setAsTimezone.localize(datetime.strptime(_data, args.timestamp))
    except Exception as e:
        print("error during timestamp-creation:")
        print(e)
        sys.exit(1)
    return _timestamp


def writeEntries(file):
    global shownPercentage, runCounter, args
    buildDataPoint = []

    for line in reversed(file):
        if not line.strip():
            continue
        if line.strip()[0].isdigit():
            _splitetData = line.split(",")

            # retrieve tags and build query for "already existing check" and build JSON for adding data
            _tags = {}
            _tagquery = ""

            if args.tags is not None:
                _splitetTag = args.tags.split(",")
                for _tag in _splitetTag:
                    _tags.update({_tag.split("=")[0]: _tag.split("=")[1]})
                    _tagquery += " AND \"%s\" = '%s'" % (_tag.split("=")[0], _tag.split("=")[1])

            # create timestamp
            _timestamp = str(createTimestamp(_splitetData)).replace(" ", "T")

            # check if entry already exists
            if not args.bulk:
                _query = ("SELECT COUNT(*) FROM %s WHERE \"time\" = '%s'" % (args.measurement, _timestamp))
                _query += _tagquery
                _existing = len(client.query(_query))
            else:
                _existing = 0

            # if entry not exists (checked by timestamp and tags) -> create it - except bulk is set. than do anyway.
            if (_existing == 0) or args.bulk:
                # build fields
                _fields = {}
                _splittetFields = args.fields.split(",")
                for num, _field in enumerate(_splittetFields):
                    # data inserted depending on tscols (timestamp-cols)
                    _fields.update({_field: float(_splitetData[num + int(tscols)])})

                # build JSON with data to write
                _buildDataPoint = [
                    {"measurement": args.measurement,
                     "time": _timestamp,
                     }
                ]

                # add fields to JSON
                _buildDataPoint[0]['fields'] = _fields

                # if given add tags to JSON
                if len(_tags) > 0:
                    _buildDataPoint[0]['tags'] = _tags

                # write entry to database one by one if not bulk
                if not args.bulk:
                    try:
                        client.write_points(_buildDataPoint)
                    except Exception as e:
                        print(e)
                        break
                else:
                    # print out progress if bulkrun
                    runCounter += 1
                    _percentageDone = calcPercent(len(file), runCounter)

                    if ((int(_percentageDone) // 5) * 5) % 5 == 0:
                        if shownPercentage != ((int(_percentageDone) // 5) * 5):
                            print("read %s %% of the values..." % ((int(_percentageDone) // 5) * 5))
                            shownPercentage = ((int(_percentageDone) // 5) * 5)

                    # add collected data to builDataPoint for later insertion to DB
                    buildDataPoint.append(_buildDataPoint[0])
            else:
                print("Entry with same tag and timestamp found in DB.")
                break
    if args.bulk:
        try:
            client.write_points(buildDataPoint)
            print("all added to DB.")
        except Exception as e:
            print(e)


def main():
    global args, client

    # display usage if no (or not enough) arguments are given
    usage()

    # display timezones if wanted
    if args.timezonehelp:
        for tz in pytz.all_timezones:
            print(tz)
        sys.exit(0)

    # get host and port
    _hostAndPort = args.host.split(":")
    if len(_hostAndPort) < 2:
        _hostAndPort.append(8086)

    # create client and try if InfluxDB server is reachable
    client = InfluxDBClient(host=_hostAndPort[0], port=_hostAndPort[1], username=args.username, password=args.password)
    try:
        client.ping()
    except Exception as e:
        print("Given host %s or port %s not reachable." % (_hostAndPort[0], _hostAndPort[1]))
        print(e)
        sys.exit(1)

    # check if DB exists
    try:
        _dbExists = next((entry for entry in client.get_list_database() if entry['name'] == args.database), False)
    except Exception as e:
        print("Something went wrong trying connecting to the database.")
        print(e)
        sys.exit(1)

    if not _dbExists:
        print("Given database %s not found or no access." % args.database)
        sys.exit(1)
    client.switch_database(args.database)

    # read CSV-File and write the entries to the DB
    resultsCSV = open(args.input, 'r')
    writeEntries(resultsCSV.readlines())

    # close db connection
    client.close()
    print("script finished.")


if __name__ == '__main__':
    main()
