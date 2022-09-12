import datetime
import json
import warnings

import certifi
import urllib3
from urllib.parse import urlencode


# -----------------------------------------------------------------------------
# class CmrProcess
#
# https://cmr.earthdata.nasa.gov/search/
# https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html
# -----------------------------------------------------------------------------
class CmrProcess(object):

    CMR_BASE_URL = 'https://cmr.earthdata.nasa.gov' +\
        '/search/granules.umm_json_v1_4?'

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
        'VIIRS-JPSS1': 'VIIRSJ1_L2_OC',
        'PO-SST': 'AVHRR_OI-NCEI-L4-GLOB-v2.1'
    }

    # These are the revisit times (hr) per mission.
    MISSION_REVISIT_TIME = {
        'MODIS-Aqua': 24,
        'CZCS': 24,
        'GOCI': 2,
        'HICO': 24,
        'OCTS': 24,
        'SeaWiFS': 24,
        'MODIS-Terra': 24,
        'VIIRS-SNPP': 24,
        'VIIRS-JPSS1': 24
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
                 error=False,
                 dayNightFlag=''):

        self._error = error
        self._dateTime = dateTime
        self._mission = mission
        self._validateLonLat(lonLat)
        self._lonLat = lonLat
        self._dayNightFlag = dayNightFlag

    # -------------------------------------------------------------------------
    # run()
    #
    # Given a set of parameters on init (time, location, mission), search for
    # the most relevant file. This uses CMR to search metadata for
    # relevant matches.
    #
    # relevancePlace is used so in the future if we ever need to query a result
    # that is not the most relevant. (E.g. The most relevant file was too
    # cloudy, etc)
    # -------------------------------------------------------------------------
    def run(self, relevancePlace=0):
        cmrRequestDictionary, self._error = self._cmrQuery()
        if self._error:
            return None, None, None, self._error
        cmrRequestDictionaryToList = list(cmrRequestDictionary.values())
        mostRelevantResult = cmrRequestDictionaryToList[relevancePlace]
        fileURL = mostRelevantResult['file_url']
        fileName = mostRelevantResult['file_name']
        return fileURL, fileName, cmrRequestDictionary, self._error

    # -------------------------------------------------------------------------
    # cmrQuery()
    #
    # Search the Common Metadata Repository(CMR) for a file that
    # is a temporal and spatial match. If no results are found, we expand
    # the temporal window to the entire temporal resolution of the image.
    # -------------------------------------------------------------------------
    def _cmrQuery(self):

        requestDictionary = self._buildRequest()
        totalHits, resultDictionary = self._sendRequest(requestDictionary)

        if self._error:
            return None, self._error

        if totalHits <= 0:
            print('No hits on original query, expanding temporal window')
            requestDictionary['temporal'] = ','.join(
                CmrProcess.buildTemporalWindow(
                    self._dateTime,
                    CmrProcess.DATE_FORMAT,
                    wholeDayFlag=False,
                    timeDelta=self.MISSION_REVISIT_TIME[self._mission]))

            totalHits, resultDictionary = self._sendRequest(requestDictionary)

            if totalHits <= 0:
                msg = 'Could not find requested mission file within' +\
                    'temporal range'
                warnings.warn(msg)
                return None, True

        resultDictionaryProcessed = self._processRequest(resultDictionary)
        return resultDictionaryProcessed, self._error

    # -------------------------------------------------------------------------
    # buildRequest()
    #
    # Build a dictionary based off of parameters given on init.
    # This dictionary will be used to encode the http request to search
    # CMR.
    # -------------------------------------------------------------------------
    def _buildRequest(self):
        temporalWindow = CmrProcess.buildTemporalWindow(self._dateTime,
                                                        self.DATE_FORMAT)
        requestDict = dict()
        requestDict['short_name'] = self.MISSION_SHORT_NAMES[self._mission]
        requestDict['point'] = ",".join(self._lonLat)
        requestDict['day_night_flag'] = self._dayNightFlag
        requestDict['temporal'] = ",".join(temporalWindow)
        return requestDict

    # -------------------------------------------------------------------------
    # _buildTemporalWindow
    #
    # Take the date and time given and create a temporal window starting with
    # beginning of the day and end of the day.
    # If the wholeDayFlag is set to false and a time delta is given, the window
    # is opened to the next closest path according to each mission.
    # -------------------------------------------------------------------------
    @staticmethod
    def buildTemporalWindow(dateTime,
                            dateFormat,
                            wholeDayFlag=True,
                            timeDelta=None):

        if wholeDayFlag:
            temporalWindowStartUnformat = dateTime.replace(hour=00,
                                                           minute=00,
                                                           second=00)
            temporalWindowEndUnformat = dateTime.replace(hour=23,
                                                         minute=59,
                                                         second=59)

        else:
            temporalWindowStartUnformat = dateTime - datetime.timedelta(
                hours=timeDelta)

            temporalWindowEndUnformat = dateTime

        temporalWindowStart = temporalWindowStartUnformat.strftime(
            dateFormat)
        temporalWindowEnd = temporalWindowEndUnformat.strftime(
            dateFormat)

        return (temporalWindowStart, temporalWindowEnd)

    # -------------------------------------------------------------------------
    # _sendRequest
    #
    # Send an http request to the CMR server.
    # Decode data and count number of hits from request.
    # -------------------------------------------------------------------------
    def _sendRequest(self, requestDictionary):
        with urllib3.PoolManager(cert_reqs='CERT_REQUIRED',
                                 ca_certs=certifi.where(),
                                 retries=urllib3.Retry(5, redirect=2),
                                 timeout=urllib3.Timeout(30)) \
                as httpPoolManager:
            encodedParameters = urlencode(requestDictionary, doseq=True)
            requestUrl = self.CMR_BASE_URL + encodedParameters

            try:
                requestResultPackage = httpPoolManager.request('GET',
                                                               requestUrl)
            except (urllib3.exceptions.MaxRetryError, Exception) as e:
                errorStr = 'Caught HTTP exception {}'.format(e)
                warnings.warn(errorStr)
                self._error = True
                return 0, None

            try:
                requestResultData = json.loads(
                    requestResultPackage.data.decode('utf-8'))
                status = int(requestResultPackage.status)
            except Exception as e:
                errorStr = 'Caught JSON unloading exception: {}'.format(e)
                warnings.warn(errorStr)
                self._error = True
                return 0, None

            if not status >= 400:
                totalHits = len(requestResultData['items'])
                return totalHits, requestResultData

            else:
                msg = 'CMR Query: Client or server error: ' + \
                    'Status: {}, Request URL: {}, Params: {}'.format(
                        str(status), requestUrl, encodedParameters)
                warnings.warn(msg)
                return 0, None

    # -------------------------------------------------------------------------
    # _processRequest
    #
    # For each result in the CMR query, unpackage relevant information to
    # a dictionary. While doing so set flags if data is not desirable (too
    # close to edge of dataset).
    #
    #  REVIEW: Make the hard-coded names class constants? There are a lot...
    # -------------------------------------------------------------------------
    def _processRequest(self, resultDict):

        resultDictProcessed = dict()

        for hit in resultDict['items']:

            fileName = hit['umm']['RelatedUrls'][0]['URL'].split(
                'getfile/')[1]

            # ---
            # These are hardcoded here because the only time these names will
            # ever change is if we changed which format of metadata we wanted
            # the CMR results back in.
            #
            # These could be placed as class constants in the future.
            # ---
            fileUrl = hit['umm']['RelatedUrls'][0]['URL']
            temporalRange = hit['umm']['TemporalExtent']['RangeDateTime']
            dayNight = hit['umm']['DataGranule']['DayNightFlag']

            if self._lonLat is not None:
                spatialExtent = hit['umm']['SpatialExten' +
                                           't']['HorizontalSpatialDom' +
                                                'ain']['Geometry']
                try:
                    withinPadding = self._checkDistanceFromPadding(
                        spatialExtent['BoundingRectangles'][0])
                except KeyError:
                    gPolygonsBoundsList = \
                        spatialExtent['GPolygons'][0]['Boundary']['Points']
                    boundingBox = \
                        self._gPolygonsToBoundingBox(gPolygonsBoundsList)
                    withinPadding = self._checkDistanceFromPadding(
                        boundingBox
                    )
            else:
                spatialExtent = 'None'
                withinPadding = True

            temporalDiff = self._calcTemporalDifference(temporalRange)
            key = (temporalDiff, withinPadding, fileName)

            resultDictProcessed[key] = {
                'file_name': fileName,
                'file_url': fileUrl,
                'temporal_range': temporalRange,
                'spatial_extent': spatialExtent,
                'day_night_flag': dayNight,
                'temporal_diff': temporalDiff,
                'within_padding': withinPadding}

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
    # _gPolygonsToBoundingBox
    #
    # Due to r2022 reprocessing, some metadata will no longer use bounding box.
    # We must calculate bounding box from list of coords of polygon.
    # -------------------------------------------------------------------------
    def _gPolygonsToBoundingBox(self, gPolygonsBoundsList):
        lats = [pair['Latitude'] for pair in gPolygonsBoundsList]
        lons = [pair['Longitude'] for pair in gPolygonsBoundsList]
        boundingBoxDict = {}
        boundingBoxDict['EastBoundingCoordinate'] = max(lons)
        boundingBoxDict['WestBoundingCoordinate'] = min(lons)
        boundingBoxDict['NorthBoundingCoordinate'] = max(lats)
        boundingBoxDict['SouthBoundingCoordinate'] = min(lats)
        return boundingBoxDict

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
    # _validateLonLat
    # -------------------------------------------------------------------------
    @staticmethod
    def _validateLonLat(lonLat):

        lon = float(lonLat[0])
        lat = float(lonLat[1])

        if not CmrProcess.LONGITUDE_RANGE[0] <= lon \
                <= CmrProcess.LONGITUDE_RANGE[1]:

            msg = 'Invalid longitude: ' + str(lon) + \
                '. Valid longitude: ' + \
                str(CmrProcess.LONGITUDE_RANGE)

            raise RuntimeError(msg)

        if not CmrProcess.LATITUDE_RANGE[0] <= lat \
                <= CmrProcess.LATITUDE_RANGE[1]:

            msg = 'Invalid latitude: ' + str(lat) + \
                '. Valid latitude: ' + \
                str(CmrProcess.LATITUDE_RANGE)

            raise RuntimeError(msg)
