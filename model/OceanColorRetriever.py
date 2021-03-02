import datetime
import os

import certifi
import json
import pandas
import urllib3
from urllib.parse import urlencode

from nepac.model.libraries.obdaac_download import httpdl


# -----------------------------------------------------------------------------
# class OceanColorRetriever
#
# https://oceandata.sci.gsfc.nasa.gov
# https://oceancolor.gsfc.nasa.gov/data/download_methods/#api
# -----------------------------------------------------------------------------
class OceanColorRetriever(object):

    CMR_BASE_URL = 'https://cmr.earthdata.nasa.gov' +\
        '/search/granules.umm_json_v1_4?'

    BASE_URL = 'oceandata.sci.gsfc.nasa.gov'

    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_DATASETS = {

        'MODIS-Aqua': ['chlor_a', 'ipar', 'Kd_490', 'par', 'pic', 'poc',
                       'Rrs_412', 'Rrs_443', 'Rrs_469', 'Rrs_488', 'Rrs_531',
                       'Rrs_547', 'Rrs_555', 'Rrs_645', 'Rrs_667', 'Rrs_678'],

        'CZCS': ['chlor_a', 'Kd_490', 'Rrs_443', 'Rrs_520', 'Rrs_550',
                 'Rrs_670'],

        'GOCI': ['chlor_a', 'Kd_490', 'poc', 'Rrs_412', 'Rrs_443', 'Rrs_490',
                 'Rrs_555', 'Rrs_660', 'Rrs_680'],

        'HICO': ['Kd_490', 'pic', 'poc', 'Rrs_353', 'Rsf_358', 'Rrs_364',
                 'Rrs_370', 'Rrs_375', 'Rrs_381', 'Rrs_387', 'Rrs_393',
                 'Rrs_398', 'Rrs_404', 'Rrs_410', 'Rrs_416', 'Rrs_421',
                 'Rrs_427', 'Rrs_433', 'Rrs_438', 'Rrs_444', 'Rrs_450',
                 'Rrs_456', 'Rrs_461', 'Rrs_467', 'Rrs_473', 'Rrs_479',
                 'Rrs_484', 'Rrs_490', 'Rrs_496', 'Rrs_501', 'Rrs_507',
                 'Rrs_513', 'Rrs_519', 'Rrs_524', 'Rrs_530', 'Rrs_536',
                 'Rrs_542', 'Rrs_547', 'Rrs_553', 'Rrs_559', 'Rrs_564',
                 'Rrs_570', 'Rrs_576', 'Rrs_582', 'Rrs_587', 'Rrs_593',
                 'Rrs_599', 'Rrs_605', 'Rrs_610', 'Rrs_616', 'Rrs_622',
                 'Rrs_627', 'Rrs_633', 'Rrs_639', 'Rrs_645', 'Rrs_650',
                 'Rrs_656', 'Rrs_662', 'Rrs_668', 'Rrs_673', 'Rrs_679',
                 'Rrs_685', 'Rrs_690', 'Rrs_696', 'Rrs_702', 'Rrs_708',
                 'Rrs_713', 'Rrs_719', 'chlor_a'],

        'OCTS': ['chlor_a', 'Kd_490', 'par', 'pic', 'poc', 'Rrs_412',
                 'Rrs_443', 'Rrs_490', 'Rrs_516', 'Rrs_565', 'Rrs_667'],

        'SeaWiFS': ['chlor_a', 'Kd_490', 'par', 'pic', 'poc', 'Rrs_412',
                    'Rrs_443', 'Rrs_490', 'Rrs_555', 'Rrs_670'],

        'MODIS-Terra': ['chlor_a', 'ipar', 'Kd_490', 'par', 'pic', 'poc',
                        'Rrs_412', 'Rrs_443', 'Rrs_469', 'Rrs_488', 'Rrs_531',
                        'Rrs_547', 'Rrs_555', 'Rrs_645', 'Rrs_667', 'Rrs_678'],

        'VIIRS-SNPP': ['chlor_a', 'Kd_490', 'par', 'pic', 'poc', 'Rrs_410',
                       'Rrs_443', 'Rrs_486', 'Rrs_551', 'Rrs_671'],

        'VIIRS-JPSS1': ['chlor_a', 'Kd_490', 'par', 'pic', 'poc', 'Rrs_411',
                        'Rrs_445', 'Rrs_489', 'Rrs_556', 'Rrs_667'],
    }

    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_DATES = {

        'MODIS-Aqua': pandas.date_range('2002-07-04', datetime.date.today()),
        'CZCS': pandas.date_range('1978-10-30', '1986-06-22'),
        'GOCI': pandas.date_range('2011-04-01', datetime.date.today()),
        'HICO': pandas.date_range('2009-09-25', '2014-09-13'),
        'OCTS': pandas.date_range('1996-11-01', '1997-06-30'),
        'SeaWiFS': pandas.date_range('1997-09-04', '2010-12-11'),
        'MODIS-Terra': pandas.date_range('2000-02-24', datetime.date.today()),
        'VIIRS-SNPP': pandas.date_range('2017-11-29', datetime.date.today()),
        'VIIRS-JPSS1': pandas.date_range('2017-11-29', datetime.date.today())
    }

    # These are the shorthand names for the missions according to CMR.
    MISSION_SHORT_NAMES = {
        'MODIS-Aqua': 'MODISA_L2_OC',
        'CZCS': 'CZCS_L2_OC',
        'GOCI': 'GOCI_L2_OC',
        'HICO': 'HICO_L2_OC',
        'OCTS': 'OCTS_L2_OC',
        'SeaWiFS': 'SeaWiFS_L2_OC',
        'MODIS-Terra': 'MODIST_L2_OC',
        'VIIRS-SNPP': 'VIIRSN_L2_OC',
        'VIIRS-JPSS1': 'VIIRSJ1_L2_OC'
    }

    # These are the revisit times (hr) per mission.
    MISSION_REVISIT_TIME = {
        'MODIS-Aqua': 384,
        'CZCS': 480,
        'GOCI': 2,
        'HICO': 24,
        'OCTS': 384,
        'SeaWiFS': 24,
        'MODIS-Terra': 384,
        'VIIRS-SNPP': 384,
        'VIIRS-JPSS1': 384
    }

    # These are substrings that we don't want to see in a CMR result.
    MISSION_INVALID_FILE_SUBSTRINGS = {
        'SeaWiFS': ['GAC']
    }

    # ---
    # This is the padding to which we can sort our results. Results that
    # have a bounding box that is too close to the edge do not yield
    # favourable results.
    # ---
    EDGE_PADDING = 2.5

    # Format to structure temporal from.
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    # Range for valid lon/lat
    LATITUDE_RANGE = (-90, 90)
    LONGITUDE_RANGE = (-180, 180)

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self,
                 mission,
                 dateTime,
                 lonLat=None,
                 dayNightFlag='',
                 outputDirectory='.'):

        self._validate(mission, dateTime, lonLat)
        self._dateTime = dateTime
        self._mission = mission
        self._lonLat = lonLat
        self._dayNightFlag = dayNightFlag
        self._outputDirectory = outputDirectory

    # -------------------------------------------------------------------------
    # run()
    #
    # Given a set of parameters on init (time, location, mission), search for
    # and download the most relevant file. This uses CMR to search metadata for
    # relevant matches, it then uses the ODBAAC download script to pull the
    # best match from the OB.DAAC.
    #
    # relevancePlace is used so in the future if we ever need to query a result
    # that is not the most relevant. (E.g. The most relevant file was too
    # cloudy, etc)
    # -------------------------------------------------------------------------
    def run(self, relevancePlace=0):
        cmrRequestDict = self._searchCMR()
        cmrRequestDictToList = list(cmrRequestDict.values())
        mostRelevantResult = cmrRequestDictToList[relevancePlace]
        fileURL = mostRelevantResult['file_url']
        fileURL = fileURL.split('.gov/cmr')[1]
        fileURL = '/ob'+fileURL
        fileName = mostRelevantResult['file_name']
        # Download the data set.
        request_status = httpdl(OceanColorRetriever.BASE_URL,
                                fileURL,
                                localpath=self._outputDirectory,
                                uncompress=True)

        # File was retrieved, or file was already present.
        if request_status == 0 or request_status == 200 \
                or request_status == 304:

            return os.path.join(self._outputDirectory,
                                fileName), cmrRequestDict

        # File not found (client error).
        else:

            msg = 'Client or server error: ' + str(request_status) + \
                '. ' + fileName

            raise RuntimeError(request_status, msg)

    # -------------------------------------------------------------------------
    # _searchCMR()
    #
    # Search the Common Metadata Repository(CMR) for a file that
    # is a temporal and spatial match. If no results are found, we expand
    # the temporal window to the entire temporal resolution of the image.
    # -------------------------------------------------------------------------
    def _searchCMR(self):

        requestDict = self._buildCMRRequestDict()
        totalHits, resultDict = self._sendCMRRequest(requestDict)

        if totalHits <= 0:

            requestDict['temporal'] = ",".join(self._makeTemporalWindow(
                wholeDayFlag=False,
                timeDelta=self.MISSION_REVISIT_TIME[self._mission]
            ))

            totalHits, resultDict = self._sendCMRRequest(requestDict)

            if totalHits <= 0:
                msg = 'Could not find requested mission file within ' +\
                    'temporal range'
                raise RuntimeError(msg)

        requestDictProcessed = self._processRequestedData(resultDict)

        return requestDictProcessed

    # -------------------------------------------------------------------------
    # _buildCMRRequestDict()
    #
    # Build a dictionary based off of parameters given on init.
    # This dictionary will be used to encode the http request to search
    # CMR.
    # -------------------------------------------------------------------------
    def _buildCMRRequestDict(self):
        temporalWindow = self._makeTemporalWindow()
        requestDict = dict()
        requestDict['short_name'] = self.MISSION_SHORT_NAMES[self._mission]
        requestDict['point'] = ",".join(self._lonLat)
        requestDict['day_night_flag'] = self._dayNightFlag
        requestDict['temporal'] = ",".join(temporalWindow)
        return requestDict

    # -------------------------------------------------------------------------
    # _makeTemporalWindow()
    #
    # Take the date and time given and create a temporal window starting with
    # beginning of the day and end of the day.
    # If the wholeDayFlag is set to false and a time delta is given, the window
    # is opened to the next closest path according to each mission.
    # -------------------------------------------------------------------------
    def _makeTemporalWindow(self, wholeDayFlag=True, timeDelta=None):

        if wholeDayFlag:
            temporalWindowStartUnformat = self._dateTime.replace(
                hour=00, minute=00, second=00)
            temporalWindowEndUnformat = self._dateTime.replace(
                hour=23, minute=59, second=59)
        else:
            temporalWindowStartUnformat = self._dateTime - datetime.timedelta(
                hours=timeDelta)
            temporalWindowEndUnformat = self._dateTime

        temporalWindowStart = temporalWindowStartUnformat.strftime(
            self.DATE_FORMAT
        )
        temporalWindowEnd = temporalWindowEndUnformat.strftime(
            self.DATE_FORMAT
        )

        return (temporalWindowStart, temporalWindowEnd)

    # -------------------------------------------------------------------------
    # _sendCMRRequest()
    #
    # Send an http request to the CMR server.
    # Decode data and count number of hits from request.
    # -------------------------------------------------------------------------
    def _sendCMRRequest(self, requestDict):
        with urllib3.PoolManager(cert_reqs='CERT_REQUIRED',
                                 ca_certs=certifi.where()) as httpPoolManager:
            encodedParameters = urlencode(requestDict, doseq=True)
            requestUrl = self.CMR_BASE_URL + encodedParameters
            requestResultPackage = httpPoolManager.request('GET',
                                                           requestUrl)
            requestResultData = json.loads(
                requestResultPackage.data.decode('utf-8'))
            totalHits = len(requestResultData['items'])
            return totalHits, requestResultData

    # -------------------------------------------------------------------------
    # _processRequestData()
    #
    # For each result in the CMR query, unpackage relevant information to
    # a dictionary. While doing so set flags if data is not desirable (too
    # close to edge of dataset).
    #
    #  REVIEW: Make the hard-coded names class constants? There are a lot...
    # -------------------------------------------------------------------------
    def _processRequestedData(self, resultDict):
        resultDictProcessed = dict()

        for hit in resultDict['items']:

            fileName = hit['umm']['RelatedUrls'][0]['URL'].split(
                'getfile/')[1]
            if self._isFileValid(fileName):
                continue

            # ---
            # These are hardcoded here because the only time these names will
            # ever change is if we changed which format of metadata we wanted
            # the CMR results back in.
            #
            # These could be placed as class constants in the future.
            # ---
            fileUrl = hit['umm']['RelatedUrls'][0]['URL']
            temporalRange = hit['umm']['TemporalExtent']['RangeDateTime']
            spatialExtent = hit['umm']['Spatial' +
                                       'Extent']['Horizontal' +
                                                 'SpatialDomain']['Geometry']
            dayNight = hit['umm']['DataGranule']['DayNightFlag']

            withinPadding = self._checkDistanceFromPadding(
                spatialExtent['BoundingRectangles'][0])
            temporalDiff = self._calcTemporalDifference(temporalRange)
            key = (temporalDiff, withinPadding, fileName)

            resultDictProcessed[key] = {
                'file_name': fileName,
                'file_url': fileUrl,
                'temporal_range': temporalRange,
                'spatial_extent': spatialExtent,
                'day_night_flag': dayNight,
                'temporal_diff': temporalDiff,
                'within_padding': withinPadding
            }

        # ---
        # Sort results by whether result is within padding (class constant)
        # first, then by time difference second.
        # ---
        sortedResultDic = {keyVal[0]: keyVal[1] for keyVal
                           in sorted(resultDictProcessed.items(),
                                     key=lambda x: (x[0][1], -x[0][0]),
                                     reverse=True)}
        return sortedResultDic

    # -------------------------------------------------------------------------
    # _isFileValid()
    #
    # If there is a substring in the filename we recieved, flag it. If not
    # leave it alone. This is meant to weed out files which are not desired.
    # Ex. S2001011122030.L2_GAC_OC.nc (We don't want a file that is a GAC
    # or Global Area Coverage file)
    # -------------------------------------------------------------------------
    def _isFileValid(self, fileName):
        if self._mission in self.MISSION_INVALID_FILE_SUBSTRINGS.keys():
            invFile = [subString for subString
                       in self.MISSION_INVALID_FILE_SUBSTRINGS[self._mission]
                       if (subString in fileName)]
            return bool(invFile)
        return False

    # -------------------------------------------------------------------------
    # _calcTemporalDifference()
    #
    # Create a dict value that is the diff in time between input file
    # and observation from instrument. Will use for relevancy.
    # -------------------------------------------------------------------------
    def _calcTemporalDifference(self, temporalRangeDict):
        try:
            temporalDiffDatetime = self._dateTime - \
                datetime.datetime.strptime(
                    temporalRangeDict['BeginningDateTime'],
                    '%Y-%m-%dT%H:%M:%S.%fZ')
            temporalDiff = abs(temporalDiffDatetime.total_seconds())
        except ValueError:
            temporalDiffDatetime = self._dateTime - \
                datetime.datetime.strptime(
                    temporalRangeDict['BeginningDateTime'],
                    '%Y-%m-%dT%H:%M:%SZ')
            temporalDiff = abs(temporalDiffDatetime.total_seconds())
        return temporalDiff

    # -------------------------------------------------------------------------
    # _checkDistanceFromPadding()
    #
    # Checks each of the edges of a result's geospatial bounding box are far
    # enough away from the given Lon/Lat. Flags the particular result if true.
    # -------------------------------------------------------------------------
    def _checkDistanceFromPadding(self, boundingBox):
        distanceFromEdgeList = []
        lon = float(self._lonLat[0])
        lat = float(self._lonLat[1])
        distanceFromEdgeList.append(
            abs(boundingBox['EastBoundingCoordinate'] - lon))
        distanceFromEdgeList.append(
            abs(boundingBox['WestBoundingCoordinate'] - lon))
        distanceFromEdgeList.append(
            abs(boundingBox['NorthBoundingCoordinate'] - lat))
        distanceFromEdgeList.append(
            abs(boundingBox['SouthBoundingCoordinate'] - lat))
        for edgeDistance in distanceFromEdgeList:
            if edgeDistance < self.EDGE_PADDING:
                return False
        return True

    # -------------------------------------------------------------------------
    # isValidDataSet()
    # -------------------------------------------------------------------------
    @ staticmethod
    def isValidDataSet(mission, dataset):
        if dataset in OceanColorRetriever.MISSION_DATASETS[mission]:
            return True
        else:
            return False

    # -------------------------------------------------------------------------
    # _validate()
    # -------------------------------------------------------------------------
    def _validate(self, mission, dateTime, lonlat):

        dateRangeNoHMS = datetime.datetime(dateTime.year,
                                           dateTime.month,
                                           dateTime.day)

        # Validate mission.
        if mission not in OceanColorRetriever.MISSION_DATASETS:

            msg = 'Invalid mission: ' + str(mission) + \
                '.  Valid missions: ' + \
                str(OceanColorRetriever.MISSION_DATASETS.keys())

            raise RuntimeError(msg)

        # Validate date is in date range of mission
        if dateRangeNoHMS not in OceanColorRetriever.MISSION_DATES[mission]:

            msg = 'Invalid date: ' + str(dateTime) + \
                '. Valid date: ' + \
                str(OceanColorRetriever.MISSION_DATES[mission])

            raise RuntimeError(msg)

        if not OceanColorRetriever.LONGITUDE_RANGE[0] <= float(lonlat[0]) \
                <= OceanColorRetriever.LONGITUDE_RANGE[1]:

            msg = 'Invalid longitude: ' + str(lonlat[0]) + \
                '. Valid longitude: ' + \
                str(OceanColorRetriever.LONGITUDE_RANGE)

            raise RuntimeError(msg)

        if not OceanColorRetriever.LATITUDE_RANGE[0] <= float(lonlat[1]) \
                <= OceanColorRetriever.LATITUDE_RANGE[1]:

            msg = 'Invalid latitude: ' + str(lonlat[1]) + \
                '. Valid latitude: ' + \
                str(OceanColorRetriever.LATITUDE_RANGE)

            raise RuntimeError(msg)
