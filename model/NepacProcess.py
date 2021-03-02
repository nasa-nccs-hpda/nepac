import csv
import datetime
import errno
import os

import numpy as np
from numpy.core.numeric import NaN
import xarray as xr

from core.model.BaseFile import BaseFile
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
class NepacProcess(object):\

    # Takes name of input csv and appends this for output file.
    RESULT_APPEND_STRING = '_output'

    # Date format to pull dates from CSV
    DATE_FORMAT = '%m/%d/%YT%H:%M:%S'

    # NetCDF Subdataset group which houses all nav data.
    NAVIGATION_GROUP = 'navigation_data'

    # NetCDF Subdataset group which houses all geophysical data.
    GEOPHYSICAL_GROUP = 'geophysical_data'

    # Amount of location difference we allow when extracting data.
    DEGREE_PATIENCE = 0.2

    # Current flag to mask out of our data extraction process.
    CURRENT_FLAG = 'land'

    # Options to mask out of our data extraction process.
    L2_FLAGS_MASKS = {
        'default_prodfail': 1073742610,
        'prodfail': 1073741824,
        'default': 786,
        'default-land': 784,
        'land': 2}

    # How the output CSV fields look.
    CSV_HEADERS = ['Time (UTC)',
                   'Date',
                   'Latitude',
                   'Longitude',
                   'CHLA (ug/L)']

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
        timeDateLocToChl = self._readInputFile()
        
        # Get the pixel values for each mission.
        rowsToWrite = []
        for i, timeDateLoc in enumerate(timeDateLocToChl):
            print('Processing row: {}/{}'.format(i+1, len(timeDateLocToChl)))
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
            ex = self._processTimeDateLoc(timeDateLoc,
                                          timeDateLocToChl[timeDateLoc],
                                          self._missions,
                                          self._outputDir)
            rowsPerTimeDateLoc = []

            for j, (missionKey, missionVals) in enumerate(ex.items()):
                for k, (rowKey, rowValues) in enumerate(missionVals.items()):

                    # First time seeing these keys, new row.
                    if j == 0:
                        newRow = []
                        rowKeyTuple = tuple(rowKey.split(","))
                        newRow.extend(list(rowKeyTuple))
                        newRow.extend(rowValues)
                        rowsPerTimeDateLoc.append(newRow)

                    # Keys are already present, append data.
                    else:
                        rowsPerTimeDateLoc[k].extend(rowValues)

            # Write this timeDate's data rows to the overall rows.
            rowsToWrite.extend(rowsPerTimeDateLoc)

        # Write the output file.
        outFileName = os.path.splitext(
            os.path.basename(self._inputFile.fileName()))
        outFileName = outFileName[0] + \
            self.RESULT_APPEND_STRING + \
            outFileName[1]

        outputFile = os.path.join(self._outputDir,
                                  outFileName)

        # Start writing to CSV
        with open(outputFile, 'w') as csvfile:

            csvwriter = csv.writer(csvfile)

            # Start with base fields
            fields = self.CSV_HEADERS

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
    # ... due to having to search spatially in addition to temporally, we
    # cannot aggregate by one combination of keys, each row that is unique
    # must be a unique entry in the dictionary.
    #
    # The result of reading the input file is an aggregated dictionary.
    # {
    #     (time1, date1, lat1, lon1): [obs1],
    #     (time2, date2, lat2, lon2): [obs2],
    #     (time3, date3, lat3, lon3): [obs3]
    # }
    # ------------------------------------------------------------------------
    def _readInputFile(self):

        timeDateLocToChl = {}

        with open(self._inputFile.fileName()) as csvFile:

            reader = csv.DictReader(csvFile)
            next(reader, None)  # Skip the line telling the number of lines.

            for row in reader:

                # ---
                # Aggregate the rows by time, date, and location. This
                # combination determines a mission file name through a CMR
                # search.
                # ---
                timeDateLocKey = (row['Time (UTC)'].strip(),
                                  row['Date'].strip(),
                                  row['Latitude'].strip(),
                                  row['Longitude'].strip())

                if timeDateLocKey not in timeDateLocToChl:
                    timeDateLocToChl[timeDateLocKey] = []

                timeDateLocToChl[timeDateLocKey]. \
                    append((row['CHLA (ug/L)'].strip()))

        return timeDateLocToChl

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
    #          (time1, data1, lat1, lon1, Chl-A1) : [pVal1, pVal2],
    #          (time2, data2, lat2, lon2, Chl-A2) : [pVal1, pVal2]
    #       }
    #   missionName2 :
    #       {
    #           (time1, data1, lat1, lon1, Chl-A1) : [pVal1, pVal2],
    #       }
    # }
    # ------------------------------------------------------------------------
    @staticmethod
    def _processTimeDateLoc(timeDateLoc, Chls, missions, outputDir):

        print('Processing', timeDateLoc)

        valuesPerMissionDict = {}

        for mission in missions:

            dictOutput = \
                NepacProcess._processMission(mission,
                                             timeDateLoc,
                                             Chls,
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
    def _processMission(mission, timeDateLoc, chls, missions, outputDir):
        print('MISSION: {}, TDL: {}'.format(
            mission, timeDateLoc))
        timeDateSplit = str(timeDateLoc[1]) + 'T' + str(timeDateLoc[0])
        dt = datetime.datetime.strptime(timeDateSplit,
                                        NepacProcess.DATE_FORMAT)
        trueLatLon = (float(timeDateLoc[2]),
                      float(timeDateLoc[3]))

        missionFile = NepacProcess.searchDownloadOceanColorRetriever(
            mission,
            dt,
            timeDateLoc
        )

        dataArrayMerged = NepacProcess.extractDataset(missionFile)
        dataSets = missions[mission]
        nepacOutputDict = {}

        x_idx, y_idx = NepacProcess.getLatLonIndex(dataArrayMerged,
                                                   trueLatLon[0],
                                                   trueLatLon[1]
                                                   )
        foundLatLon = (float(dataArrayMerged.latitude[x_idx][y_idx]),
                       float(dataArrayMerged.longitude[x_idx][y_idx])
                       )

        print('Given location: Lat={} Lon={}'.format(
            trueLatLon[0], trueLatLon[1]))
        print('Closes location: Lat={} Lon={}'.format(
            foundLatLon[0],
            foundLatLon[1]
        ))

        latlonFoundOutOfBounds = NepacProcess.checkLatLonOutOfWindow(
            trueLatLon,
            foundLatLon)

        for sub in sorted(dataArrayMerged.variables):

            datasetName = sub

            if datasetName in dataSets:

                val = dataArrayMerged[datasetName].sel(number_of_lines=x_idx,
                                                       pixels_per_line=y_idx)

                # Return NAN if closest location not within our window.
                val = float(val) if not latlonFoundOutOfBounds else NaN

                timeDateLocChlKey = (
                    timeDateLoc[0],
                    timeDateLoc[1],
                    timeDateLoc[2],
                    timeDateLoc[3],
                    chls[0])

                timeDateLocChlKey = ','.join(timeDateLocChlKey)

                # New key to be made, appends vals
                if timeDateLocChlKey not in nepacOutputDict:

                    nepacOutputDict[timeDateLocChlKey] = []
                    nepacOutputDict[timeDateLocChlKey].append(val)

                # Key has already been added, append vals
                else:

                    nepacOutputDict[timeDateLocChlKey].append(val)

        if os.path.exists(missionFile):

            os.remove(missionFile)

        nepacMissionOutput = {}
        nepacMissionOutput[mission] = nepacOutputDict

        return nepacMissionOutput

    # ------------------------------------------------------------------------
    # searchDownloadOceanColorRetriever()
    #
    # Intantiate and run an OceanColorRetriever object which will query
    # and download the appropriate OB DAAC NetCDF file.
    #
    # If the file was not properly downloaded but no error was raised in
    # running OCR, we can conclude that a concurrency problem occured
    # from CELERY with an improper download. Try it again. The assumption is
    # that the error case would likely never be encountered during a
    # linear run, only a parallel run should we find something like this.
    # ------------------------------------------------------------------------
    @staticmethod
    def searchDownloadOceanColorRetriever(mission, dt, timeDateLoc):
        ocr = OceanColorRetriever(mission=mission,
                                  dateTime=dt,
                                  lonLat=(timeDateLoc[3], timeDateLoc[2]),
                                  dayNightFlag='')
        missionFile, _ = ocr.run()
        if not os.path.exists(missionFile):
            print('{} was not properly downloaded.\n Retrying'.format(
                missionFile))
            ocr = OceanColorRetriever(mission=mission,
                                      dateTime=dt,
                                      lonLat=(timeDateLoc[3], timeDateLoc[2]),
                                      dayNightFlag='')
            missionFile, _ = ocr.run()
            if not os.path.exists(missionFile):
                msg = '{} not succesfully downloaded'.format(missionFile)
                raise RuntimeError(msg)
        return missionFile

    # ------------------------------------------------------------------------
    # extractDataset()
    #
    # Extract the given mission file to an xarray ndarray. Merge necessary
    # sub-datasets (LAT/LON/L2_FLAGS/Bands)
    # ------------------------------------------------------------------------

    @staticmethod
    def extractDataset(missionFile):
        dataArrayGeo = xr.open_dataset(missionFile,
                                       group=NepacProcess.GEOPHYSICAL_GROUP)
        dataArrayNav = xr.open_dataset(missionFile,
                                       group=NepacProcess.NAVIGATION_GROUP)
        dataArrayMerged = xr.merge(
            [dataArrayNav.latitude,
             dataArrayNav.longitude,
             dataArrayGeo])

        # ---
        # FOR REVIEW: This is my C brain coming out. Is this necessary
        # and/or desired to trigger garbage collection?
        # ---
        dataArrayGeo = None
        dataArrayNav = None
        return dataArrayMerged

    # ------------------------------------------------------------------------
    # getLatLonIndex()
    #
    # Given an xarray ndarray (dataset), and a given location, find the
    # closest valid location.
    # Valid is defined as what we're not looking for (no cloud/ice, no land).
    # ------------------------------------------------------------------------
    @staticmethod
    def getLatLonIndex(xrDset, lat, lon):
        nanarray = np.empty(xrDset.l2_flags.shape)
        nanarray.fill(np.nan)
        condition = (
            (xrDset.l2_flags &
             NepacProcess.L2_FLAGS_MASKS[NepacProcess.CURRENT_FLAG])
            > 0)
        condLats = np.where(condition,
                            nanarray, xrDset.latitude)
        condLons = np.where(condition,
                            nanarray, xrDset.longitude)
        diffSquared = (condLats - lat)**2 + (condLons - lon)**2
        argm = np.nanargmin(diffSquared)
        x_loc, y_loc = np.unravel_index(argm, diffSquared.shape)
        return x_loc, y_loc

    # ------------------------------------------------------------------------
    # checkLatLonOutOfWindow()
    #
    # Given a ground truth location (trueLatLon) and a location found via
    # getLatLonIndex(), determine if any valid solutions were found.
    #
    # Validity is determined by a window (NepacProcess.DEGREE_PATRIENCE).
    # ------------------------------------------------------------------------
    @staticmethod
    def checkLatLonOutOfWindow(trueLatLon, foundLatLon):
        latlonFoundOutOfBounds = False
        for i, latLon in enumerate(trueLatLon):
            abs_diff = abs(latLon - foundLatLon[i])
            if abs_diff > NepacProcess.DEGREE_PATIENCE:
                latlonFoundOutOfBounds = True
                print('No valid location found (out of bounds)')
                print('Distance difference: {}'.format(abs_diff))
        return latlonFoundOutOfBounds
