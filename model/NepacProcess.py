import csv
import datetime
import errno
import os
import tempfile
import struct

from osgeo.osr import SpatialReference
from osgeo.gdal import GDT_Float32
from osgeo.gdal import Translate

from core.model.BaseFile import BaseFile
from core.model.GeospatialImageFile import GeospatialImageFile
from nepac.model.OceanColorRetriever import OceanColorRetriever


# -----------------------------------------------------------------------------
# class NepacProcess
#
# https://oceandata.sci.gsfc.nasa.gov
# https://oceancolor.gsfc.nasa.gov/data/download_methods/#api
#
# Nepac uses its own format of an observation file.  As we do not foresee much
# manipulation of it beyond adding the independent variable pixel values, we
# will not create a derivation of ObservationFile.  Instead, we will simply
# process the input file line by line.  This strategy works fine with
# distributed processing, which is forthcoming in another class.
#
# Two rows could require the same data set because they could have the same
# time, date.  The lat/lon combination could be different.  We could do a bunch
# of bookkeeping to aggregate the rows by time and date, or we can rely on
# OceanColorRetriever not to bother downloading, if the requested file exists.
# We would still need to open the file multiple times.
#
# How would the aggregation table look?
# time, date, [(lat, lon), (lat, lon), ...]
# time, date, [(lat, lon), (lat, lon), ...]
# ...
# -----------------------------------------------------------------------------
class NepacProcess(object):

    # -------------------------------------------------------------------------
    # __init__
    #
    # The input file contains the observations.  The data sets to add are in
    # missionDataSetDict.
    # -------------------------------------------------------------------------
    def __init__(self, nepacInputFile, missionDataSetDict, outputDir):

        if not isinstance(nepacInputFile, BaseFile):

            self._inputFile = BaseFile(nepacInputFile)

        else:
            self._inputFile = nepacInputFile

        if not outputDir:

            raise ValueError('An output directory must be specified.')

        if not os.path.isdir(outputDir):

            raise FileNotFoundError(errno.ENOENT,
                                    os.strerror(errno.ENOENT),
                                    outputDir)

        self._outputDir = outputDir
        self._validateMissionDataSets(missionDataSetDict)
        self._missions = missionDataSetDict

    # -------------------------------------------------------------------------
    # validateMissionDataSets
    # -------------------------------------------------------------------------
    def _validateMissionDataSets(self, missionDataSetDict):

        if missionDataSetDict is None:

            raise ValueError('A mission-to-data-set dictionary must be' +
                             ' provided.')

        for mission in missionDataSetDict:
            for dataSet in missionDataSetDict[mission]:
                if not OceanColorRetriever.isValidDataSet(mission, dataSet):

                    msg = 'Invalid data set, ' + \
                          str(dataSet) + \
                          ' for mission ' + \
                          str(mission)

                    raise ValueError(msg)

    # -------------------------------------------------------------------------
    # run
    #
    # The data returned from self._processTimeData is on a per-mission basis.
    # This is done for the idea that it doesn't matter whi
    # -------------------------------------------------------------------------
    def run(self):

        # Read the input file and aggregate by mission.
        timeDateToLocChl = self._readInputFile()

        # Get the pixel values for each mission.
        rowsToWrite = []

        for timeDate in timeDateToLocChl:

            # ----------------------------------------------------------------
            # The data returned from self._processTimeDate takes the form of:
            # { missionName1 :
            #      {
            #          (time1, data1, lat1, long1, Chl-A1) : [pVal1, pVal2],
            #          (time2, data2, lat2, long2, Chl-A2) : [pVal1, pVal2]
            #       }
            #   missionName2 :
            #       {
            #           (time1, data1, lat1, long1, Chl-A1) : [pVal1, pVal2],
            #       }
            # }
            # This data structure needs to be reduced to one key per row with
            # aggregated values.
            #
            # [time1, date1, lat1, long1, Chl-A1,
            #   Mission1-pVal1, Mission1-pVal2, Mission2-pVal2]
            # [time2, date2, lat2, long2, Chl-a2,
            #   Mission1-pVal1, Mission1-pVal2, Mission2-pVal2]
            # ----------------------------------------------------------------

            ex = self._processTimeDate(timeDate,
                                       timeDateToLocChl[timeDate],
                                       self._missions,
                                       self._outputDir)
            rowsPerTimeDate = []

            for i, (missionKey, missionVals) in enumerate(ex.items()):
                for k, (rowKey, rowValues) in enumerate(missionVals.items()):

                    # First time seeing these keys, new row.
                    if i == 0:
                        newRow = []
                        rowKeyTuple = tuple(rowKey.split(","))
                        newRow.extend(list(rowKeyTuple))
                        newRow.extend(rowValues)
                        rowsPerTimeDate.append(newRow)

                    # Keys are already present, append data.
                    else:
                        rowsPerTimeDate[k].extend(rowValues)

            # Write this timeDate's data rows to the overall rows.
            rowsToWrite.extend(rowsPerTimeDate)

        # Write the output file.
        outFileName = os.path.splitext(
            os.path.basename(self._inputFile.fileName()))
        outFileName = outFileName[0] + '_output' + outFileName[1]

        outputFile = os.path.join(self._outputDir,
                                  outFileName)

        # Start writing to CSV
        with open(outputFile, 'w') as csvfile:

            csvwriter = csv.writer(csvfile)

            # Start with base fields
            fields = ['Time[hhmm]',
                      'Date[mmddyyyy]',
                      'Lat.[-90—90 deg.]',
                      'Long.[0-360.E]',
                      'Chl-a']

            # Sort keys in missions to match incoming data, add to fields.
            for mission in sorted(self._missions.keys()):
                for subDataSet in sorted(self._missions[mission]):
                    fields.append(str(mission+'-'+subDataSet))

            csvwriter.writerow(fields)
            csvwriter.writerows(rowsToWrite)

    # ------------------------------------------------------------------------
    # _readInputFile
    #
    # The input file looks like, ...
    #
    # time1, date1, lat1, lon1, obs1
    # time2, date2, lat2, lon2, obs2
    # time1, date1, lat2, lon1, obs3
    #
    # ... and the DAAC's file names are based on date and time, so aggregate
    # the observations by date and time.  That allows us to get pixel values
    # for every observation location in the same image file with a single
    # read.
    #
    # The result of reading the input file is an aggregated dictionary.
    # {
    #     (time1, date1): [(lat1, lon1, obs1), (lat2, lon1, obs3)],
    #     (time2, date2): [(lat2, lon2, obs2)]
    # }
    # ------------------------------------------------------------------------
    def _readInputFile(self):

        timeDateToLocChl = {}

        with open(self._inputFile.fileName()) as csvFile:

            reader = csv.DictReader(csvFile)
            next(reader, None)  # Skip the line telling the number of lines.

            for row in reader:

                # ---
                # Aggregate the rows by time and date.  Time and date
                # determine a mission file name.  There could be multiple
                # observations for a single time and date combination.  Get
                # the imagery once, and extract all the values at the
                # observation points.
                # ---
                timeDateKey = (row['Time[hhmm]'].strip(),
                               row['Date[mmddyyyy]'].strip())

                if timeDateKey not in timeDateToLocChl:
                    timeDateToLocChl[timeDateKey] = []

                timeDateToLocChl[timeDateKey]. \
                    append((row['Lat.[-90—90 deg.]'].strip(),
                            row['Long.[0-360.E]'].strip(),
                            row['Chl-a'].strip()))

        return timeDateToLocChl

    # ------------------------------------------------------------------------
    # processTimeDate
    #
    # For each row in the observations, or each time/date in the aggregate,
    # every mission the user requested must be retrieved.  Later, the
    # requested will be data sets extracted, and the pixel values retrieved.
    #
    # This method can be distributed.
    #
    # Data from self._processMission is returned in a per-key manner, ...
    #
    # { (time, data, lat, long, Chl-A) : [pVal1, pVal2, pVal3]}
    #
    # This is added to a per-mission dictionary, ...
    #
    # { missionName1 :
    #      {
    #          (time1, data1, lat1, long1, Chl-A1) : [pVal1, pVal2],
    #          (time2, data2, lat2, long2, Chl-A2) : [pVal1, pVal2]
    #       }
    #   missionName2 :
    #       {
    #           (time1, data1, lat1, long1, Chl-A1) : [pVal1, pVal2],
    #       }
    # }
    # ------------------------------------------------------------------------
    @staticmethod
    def _processTimeDate(timeDate, locsChls, missions, outputDir):

        print('Processing', timeDate)

        valuesPerMissionDict = {}

        for mission in missions:

            dictOutput = \
                NepacProcess._processMission(mission,
                                             timeDate,
                                             locsChls,
                                             missions,
                                             outputDir)
            valuesPerMissionDict.update(dictOutput)

        sortedValuesPerMissionDict = dict(
            sorted(valuesPerMissionDict.items(),
                   key=lambda item: item[0]))

        return sortedValuesPerMissionDict

    # ------------------------------------------------------------------------
    # processMission
    #
    # This can be distributed.
    # ------------------------------------------------------------------------
    @staticmethod
    def _processMission(mission, timeDate, locsChls, missions, outputDir):

        print('Processing mission', mission)
        print('timeDate: ', timeDate)
        print('locs and chl: ', locsChls)
        # Get the image file.
        hour = int(timeDate[0][:2])
        minute = int(timeDate[0][2:])
        month = int(timeDate[1][:2])
        day = int(timeDate[1][2:4])
        year = int(timeDate[1][4:])
        dt = datetime.datetime(year, month, day, hour, minute)

        ocr = OceanColorRetriever(mission, dt, outputDir)
        missionFile = ocr.run()

        # Instantiate a GeospatialImageFile to access its data sets.
        srs = SpatialReference()
        srs.ImportFromEPSG(4326)
        image = GeospatialImageFile(missionFile, srs)
        subs = image._getDataset().GetSubDatasets()

        # ---
        # Extract the data sets and get the pixel values.  This should
        # probably not be distributed.  There would be multiple processes
        # trying to open the same image.
        # ---
        dataSets = missions[mission]
        nepacOutputDict = {}

        for sub in sorted(subs):

            datasetName = sub[0]
            var = datasetName.split(':')[2]
            cleanedVar = datasetName.split('_data/')[1]

            print('Seeking', var, 'in', dataSets)

            if cleanedVar in dataSets:

                print('Found', var, 'in', dataSets)

                name = tempfile.mkstemp()[1]

                Translate(name, datasetName)
                dsImage = GeospatialImageFile(name, srs)

                for locChl in locsChls:

                    imagePt = dsImage.groundToImage(float(locChl[0]),
                                                    float(locChl[1]))

                    val = dsImage._getDataset().\
                        ReadRaster(imagePt[0],
                                   imagePt[1],
                                   xsize=1,
                                   ysize=1,
                                   buf_type=GDT_Float32)

                    # Byte form to float
                    [val] = struct.unpack('f', val)

                    timeDateLocChlKey = (
                        timeDate[0],
                        timeDate[1],
                        locChl[0],
                        locChl[1],
                        locChl[2])
                    timeDateLocChlKey = ','.join(timeDateLocChlKey)

                    # New key to be made, appends vals
                    if timeDateLocChlKey not in nepacOutputDict:

                        nepacOutputDict[timeDateLocChlKey] = []
                        nepacOutputDict[timeDateLocChlKey].append(val)

                    # Key has already been added, append vals
                    else:
                        nepacOutputDict[timeDateLocChlKey].append(val)

                dsImage = None
                os.remove(name)

        if os.path.exists(missionFile):
            os.remove(missionFile)

        nepacMissionOutput = {}
        nepacMissionOutput[mission] = nepacOutputDict

        return nepacMissionOutput
