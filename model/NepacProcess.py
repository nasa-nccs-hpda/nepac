
import csv
import datetime
import errno
import os
import shutil
import tempfile

from osgeo.osr import SpatialReference

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
    # -------------------------------------------------------------------------
    def run(self):

        # Read the input file and aggregate by mission.
        timeDateToLoc = self._readInputFile()

        # Get the pixel values for each mission.
        for timeDate in timeDateToLoc:
            self._processTimeDate(timeDate, timeDateToLoc[timeDate])

        # Write the output file.

    # -------------------------------------------------------------------------
    # _readInputFile
    #
    # The input file looks like, ...
    #
    # time1, date1, lat1, lon1, observation1
    # time2, date2, lat2, lon2, observation2
    # time1, date1, lat2, lon1, observation3
    #
    # ... and the DAAC's file names are based on date and time, so aggregate
    # the observations by date and time.  That allows us to get pixel values
    # for every observation location in the same image file with a single
    # read.
    #
    # The result of reading the input file is an aggregated dictionary.
    # {
    #     (time1, date1): [(lat1, lon1), (lat2, lon1)],
    #     (time2, date2): [(lat2, lon2)]
    # }
    # -------------------------------------------------------------------------
    def _readInputFile(self):

        timeDateToLoc = {}

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

                if timeDateKey not in timeDateToLoc:
                    timeDateToLoc[timeDateKey] = []

                timeDateToLoc[timeDateKey]. \
                    append((row['Lat.[-90—90 deg.]'].strip(),
                            row['Long.[0-360.E]'].strip()))

        return timeDateToLoc

    # -------------------------------------------------------------------------
    # processTimeDate
    #
    # For each row in the observations, or each time/date in the aggregate,
    # every mission the user requested must be retrieved.  Later, the
    # requested will be data sets extracted, and the pixel values retrieved.
    #
    # This method can be distributed.
    # -------------------------------------------------------------------------
    def _processTimeDate(self, timeDate, locs):

        print('Processing', timeDate)

        for mission in self._missions:
            self._processMission(mission, timeDate, locs)

    # -------------------------------------------------------------------------
    # processMission
    #
    # This can be distributed.
    # -------------------------------------------------------------------------
    def _processMission(self, mission, timeDate, locs):

        print('Processing mission', mission)

        # Get the image file.
        hour = int(timeDate[0][:2])
        minute = int(timeDate[0][2:])
        month = int(timeDate[1][:2])
        day = int(timeDate[1][2:4])
        year = int(timeDate[1][4:])
        dt = datetime.datetime(year, month, day, hour, minute)

        ocr = OceanColorRetriever(mission, dt, self._outputDir)
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
        dsDir = tempfile.gettempdir()
        dataSets = self._missions[mission]

        for sub in subs:

            datasetName = sub[0]
            var = datasetName.split(':')[2]
            print('Seeking', var, 'in', dataSets)

            if var in dataSets:

                print('Found', var, 'in', dataSets)
                name = os.path.join(dsDir, var + '.nc')
                dsImage = GeospatialImageFile(name, srs)

                for loc in locs:

                    imagePt = dsImage.groundToImage(loc[0], loc[1])

                    val = dsImage._getDataset().ReadRaster(imagePt[0],  # x
                                                           imagePt[1],  # y
                                                           1,
                                                           1)

                dsImage = None
                shutil.remove(name)
