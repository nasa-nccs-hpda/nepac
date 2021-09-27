import csv
import datetime
import glob
import errno
import math
import os
import warnings

from core.model.BaseFile import BaseFile
from nepac.model.Retriever import Retriever
from nepac.model.BosswRetriever import BosswRetriever
from nepac.model.EtopoRetriever import EtopoRetriever
from nepac.model.OcSWFHICOCTRetriever import OcSWFHICOCTRetriever
from nepac.model.OceanColorRetriever import OceanColorRetriever
from nepac.model.OccciRetriever import OccciRetriever
from nepac.model.OisstRetriever import OisstRetriever
from nepac.model.PosstRetriever import PosstRetriever


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

    # Maps each mission to its' specific retriever.
    OBJECT_DICTIONARY = {
        'MODIS-Aqua': OceanColorRetriever,
        'CZCS': OceanColorRetriever,
        'GOCI': OceanColorRetriever,
        'HICO': OcSWFHICOCTRetriever,
        'OCTS': OcSWFHICOCTRetriever,
        'SeaWiFS': OcSWFHICOCTRetriever,
        'MODIS-Terra': OceanColorRetriever,
        'VIIRS-SNPP': OceanColorRetriever,
        'VIIRS-JPSS1': OceanColorRetriever,
        'OC-CCI': OccciRetriever,
        'OI-SST': OisstRetriever,
        'BO-SSW': BosswRetriever,
        'PO-SST': PosstRetriever,
        'ETOPO1-BED': EtopoRetriever,
        'ETOPO1-ICE': EtopoRetriever
    }

    # OB DAAC sensors which are not populated in NASA Earth's CMR.
    NON_CMR_SENSORS = ['OCTS', 'SeaWiFS', 'HICO']

    # Placeholder index variable if no valid location was found.
    NO_DATA_IDX = -1

    # Amount of rows to process in one round.
    CHUNK_SIZE = 100

    # -------------------------------------------------------------------------
    # __init__
    #
    # The input file contains the observations.  The data sets to add are in
    # missionDataSetDict.
    # -------------------------------------------------------------------------
    def __init__(self, nepacInputFile, missionDataSetDict, outputDir,
                 dummyPath, noData, erroredData):

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
        if not os.path.isdir(dummyPath):
            raise FileNotFoundError(errno.ENOENT,
                                    os.strerror(errno.ENOENT),
                                    dummyPath)

        self._outputDir = outputDir
        self._validateMissionDataSets(missionDataSetDict)
        self._missions = missionDataSetDict
        self._dummyPath = dummyPath
        self._noData = noData
        self._erroredData = erroredData

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
    # This method calls sub-methods to (1) read input file into memory, (2)
    # search, download, extract raster data from the desired files, (3)
    # formats and writes data extracted to file.
    # -------------------------------------------------------------------------
    def run(self):
        # Read the input file and aggregate by mission.
        timeDateLocToChl = self._readInputFile()

        # Write the output file.
        outFileName = os.path.splitext(
            os.path.basename(self._inputFile.fileName()))
        outFileName = outFileName[0] + \
            self.RESULT_APPEND_STRING + \
            outFileName[1]

        outputFile = os.path.join(self._outputDir,
                                  outFileName)
        self._initializeCSV(outputFile)
        chunkedDict = self._splitDict(timeDateLocToChl,
                                      NepacProcess.CHUNK_SIZE)
        numChunks = len(chunkedDict)
        for i, chunk in enumerate(chunkedDict):
            print('Processing chunk {} of {}'.format(i+1, numChunks))
            self._process(chunk, outputFile)
        NepacProcess.removeNCFiles()

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
                dateTimeRow = row['DateTime'].strip()
                dateTimeRowWithSecond = str(dateTimeRow)+':00'
                dtFormat = datetime.datetime.strptime(dateTimeRowWithSecond,
                                                      '%Y-%m-%dT%H:%M:%S')
                time = dtFormat.strftime('%H:%M:%S')
                date = dtFormat.strftime('%m/%d/%Y')

                try:
                    lat = row['\ufeffLat'].strip()
                except KeyError:
                    lat = row['Lat'].strip()

                timeDateLocKey = (time,
                                  date,
                                  lat,
                                  row['Lon'].strip())

                if timeDateLocKey not in timeDateLocToChl:
                    timeDateLocToChl[timeDateLocKey] = []

                timeDateLocToChl[timeDateLocKey]. \
                    append((row['Chla_all'].strip()))

        return timeDateLocToChl

    # -------------------------------------------------------------------------
    # initializeCSV
    # -------------------------------------------------------------------------
    def _initializeCSV(self, outputFile):
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

    # -------------------------------------------------------------------------
    # splitDict
    # -------------------------------------------------------------------------
    def _splitDict(self, dictInput, n):
        keys = list(dictInput.keys())
        splitKeys = [keys[i: i+n] for i in range(0, len(keys), n)]
        outputList = []
        for chunk in splitKeys:
            newDict = {}
            for key in chunk:
                newDict[key] = dictInput[key]
            outputList.append(newDict)
        return outputList

    # -------------------------------------------------------------------------
    # process
    # -------------------------------------------------------------------------
    def _process(self, timeDateLocToChl, outputFile):
        # Get the pixel values for each mission.
        rowsToWrite = []

        for i, timeDateLoc in enumerate(timeDateLocToChl):

            # print('Processing row: {}/{}'.format(i+1, len(timeDateLocToChl)))

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
                                          self._outputDir,
                                          self._dummyPath,
                                          noDataValue=self._noData,
                                          erroredDataValue=self._erroredData)
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
        with open(outputFile, 'a') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerows(rowsToWrite)

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
    def _processTimeDateLoc(timeDateLoc, Chls, missions, outputDir,
                            dummyPath, noDataValue=9999,
                            erroredDataValue=9998):

        print('Processing', timeDateLoc)

        valuesPerMissionDict = {}

        for mission in missions:

            dictOutput = \
                NepacProcess._processMission(mission,
                                             timeDateLoc,
                                             Chls,
                                             missions,
                                             outputDir,
                                             dummyPath,
                                             noDataValue=noDataValue,
                                             erroredDataValue=erroredDataValue)
            valuesPerMissionDict.update(dictOutput)

        sortedValuesPerMissionDict = dict(
            sorted(valuesPerMissionDict.items(),
                   key=lambda item: item[0]))

        return sortedValuesPerMissionDict

    # ------------------------------------------------------------------------
    # _processMission()
    #
    # Method to:
    # (a) Determine the correct Retriever object based off of
    # mission.
    # (b) Run the retriever to find, download, and extract data.
    # (c) Check for any errors encountered in retrieval process.
    # (d) Construct a data packet to return which contains pixel vals per
    # dataset requested, or if an error occured, place a user-given
    # value as the pixel value.
    # ------------------------------------------------------------------------
    @staticmethod
    def _processMission(mission, timeDateLoc, chls, missions, outputDir,
                        dummyPath, noDataValue=9999, erroredDataValue=9998):
        print('MISSION: {}, TDL: {}'.format(mission, timeDateLoc))
        xIdx = None
        yIdx = None
        latLonFound = True

        timeDateSplit = str(timeDateLoc[1]) + 'T' + str(timeDateLoc[0])

        dt = datetime.datetime.strptime(timeDateSplit,
                                        NepacProcess.DATE_FORMAT)

        trueLatLon = (float(timeDateLoc[2]),
                      float(timeDateLoc[3]))
        retrieverLonLat = (timeDateLoc[3],
                           timeDateLoc[2])

        retrieverObject = NepacProcess.OBJECT_DICTIONARY[mission](
            mission,
            dt,
            dummyPath,
            retrieverLonLat)

        dataset, _, retrieverError = retrieverObject.run()

        dataSets = missions[mission]
        nepacOutputDict = {}

        # Mission requires geo-locating.
        if not retrieverObject.GEOREFERENCED:

            # ---
            # Mission requires special retriever to search through
            # multiple files to find the correct orbit. Since files
            # are already being geolocated we can get the correct indices
            # now rather than geolocating again with other sensors.
            # ---
            if mission in NepacProcess.NON_CMR_SENSORS:

                xIdx = retrieverObject.xIdx
                yIdx = retrieverObject.yIdx

            else:

                # ---
                # Given a non-georeferenced dataset, geolocate the specific
                # point to look for. Return the indices, and whether the
                # closest valid location was within a spatial window.
                # ---
                try:
                    xIdx, yIdx = Retriever.geoLocate(
                        dataset, trueLatLon[0], trueLatLon[1], quiet=True,
                        error=retrieverError)
                except Exception as e:
                    errorStr = 'Error geolocating, using no-data value. ' +\
                        'Error: {}'.format(e)
                    xIdx = NepacProcess.NO_DATA_IDX
                    yIdx = NepacProcess.NO_DATA_IDX
                    warnings.warn(errorStr)

            if xIdx == NepacProcess.NO_DATA_IDX and \
                    yIdx == NepacProcess.NO_DATA_IDX:
                retrieverError = True

        for sub in sorted(dataset.variables):

            datasetName = sub

            if datasetName in dataSets:

                # We need to sample pixel via indices.
                if not retrieverObject.GEOREFERENCED:
                    try:
                        val = dataset[datasetName].sel(number_of_lines=xIdx,
                                                       pixels_per_line=yIdx)
                        val = float(val)
                    except Exception as e:
                        val = float(erroredDataValue)
                        retrieverError = True
                        warningStr = 'Error in finding file or opening in' +\
                            'Xarray. Using no-data value. Error: {}'.format(e)
                        warnings.warn(warningStr)

                    # ---
                    # If any flags were thrown, write out no-data number
                    # or errored-pixel number.
                    # ---
                    val = float(erroredDataValue) if retrieverError \
                        else val
                    val = float(noDataValue) if math.isnan(val) \
                        or not latLonFound \
                        else val

                # We need to sample pixel via lat,lon (L3/L4 data).
                else:
                    try:
                        val = dataset[datasetName].sel(lat=trueLatLon[0],
                                                       lon=trueLatLon[1],
                                                       method='nearest')
                        val = float(val)
                    except Exception as e:
                        val = float(erroredDataValue)
                        retrieverError = True
                        warningStr = 'Error in finding file or opening in' +\
                            'Xarray. Using no-data value. Error: {}'.format(e)
                        warnings.warn(warningStr)

                    val = float(erroredDataValue) if retrieverError \
                        else val
                    val = float(noDataValue) if math.isnan(val) \
                        else val

                # Some sensors require a function to be applied to the val.
                if retrieverObject.SPECIAL_VALUE_FUNCTION:
                    val = retrieverObject.retrieverValueFunction(val) if \
                        not retrieverError else val

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

        nepacMissionOutput = {}
        nepacMissionOutput[mission] = nepacOutputDict
        return nepacMissionOutput

    # ------------------------------------------------------------------------
    # removeNCFiles()
    # ------------------------------------------------------------------------
    @staticmethod
    def removeNCFiles():
        ncFileList = [fv for fv in glob.glob('*.nc')]
        for ncFile in ncFileList:
            os.remove(ncFile)
