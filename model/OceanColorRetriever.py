import datetime
import os

import pandas

from nepac.model.libraries.obdaac_download import httpdl

# -----------------------------------------------------------------------------
# class OceanColorRetriever
#
# https://oceandata.sci.gsfc.nasa.gov
# https://oceancolor.gsfc.nasa.gov/data/download_methods/#api
# -----------------------------------------------------------------------------


class OceanColorRetriever(object):

    BASE_URL = 'oceandata.sci.gsfc.nasa.gov'

    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_DATASETS = {

        'MODIS-Aqua': ['iPAR', 'Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_412',
                       'Rrs_443', 'Rrs_469', 'Rrs_488', 'Rrs_531', 'Rrs_547',
                       'Rrs_555', 'Rrs_645', 'Rrs_667', 'Rrs_678'],

        'CZCS': ['Kd_490', 'Rrs_520', 'Rrs_543', 'Rrs_550', 'Rrs_670'],

        'GOCI': ['Kd_490', 'POC', 'Rrs_412', 'Rrs_443', 'Rrs_490', 'Rrs_555',
                 'Rrs_660', 'Rrs_680'],

        'HICO': ['Kd_490', 'PIC', 'POC', 'Rrs_353', 'Rsf_358', 'Rrs_364',
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
                 'Rrs_713', 'Rrs_719'],

        'OCTS': ['Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_412', 'Rrs_443',
                 'Rrs_490', 'Rrs_516', 'Rrs_565', 'Rrs_667'],

        'SeaWiFS': ['Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_412', 'Rrs_443',
                    'Rrs_490', 'Rrs_555', 'Rrs_670'],

        'MODIS-Terra': ['iPAR', 'Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_412',
                        'Rrs_443', 'Rrs_469', 'Rrs_488', 'Rrs_531', 'Rrs_547',
                        'Rrs_555', 'Rrs_645', 'Rrs_667', 'Rrs_678'],

        'VIIRS-SNPP': ['Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_411', 'Rrs_445',
                       'Rrs_489', 'Rrs_556', 'Rrs_667'],

        'VIIRS-JPSS1': ['Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_411', 'Rrs_445',
                        'Rrs_489', 'Rrs_556', 'Rrs_667'],
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

    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_FILE_SUFFIXES = {

        'MODIS-Aqua': '_LAC_OC',
        'CZCS': '_MLAC_OC',
        'GOCI': '_COMS_OC',
        'HICO': '_ISS_OC',
        'OCTS': '_GAC_OC',
        'SeaWiFS': '_GAC_OC',
        'MODIS-Terra': '_LAC_OC',
        'VIIRS-SNPP': '_SNPP_OC',
        'VIIRS-JPSS1': '.SST'
    }

    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_LEVEL = 'L2'

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self, mission, dateTime, outputDirectory='.'):

        self._validate(mission, dateTime)
        self._dateTime = dateTime
        self._mission = mission
        self._outputDirectory = outputDirectory

    # -------------------------------------------------------------------------
    # run
    # -------------------------------------------------------------------------
    def run(self):

        if self._mission == 'MODIS-Aqua' or self._mission == 'MODIS-Terra':
            fileName = ''.join(
                self._mission[6:7])
        else:
            fileName = ''.join(
                self._mission[:1])

        fileName = fileName + self._dateTime.strftime("%Y") + \
            self._dateTime.strftime("%j") + \
            self._dateTime.strftime("%H") + \
            self._dateTime.strftime("%M") + \
            self._dateTime.strftime("%S") + \
            '.' + OceanColorRetriever.MISSION_LEVEL + \
            OceanColorRetriever.MISSION_FILE_SUFFIXES[self._mission] + \
            '.nc'

        # -----------------------------------------------------------------------------
        # Even though it appears you need a full url including-
        # date, year, etc., all files are downloaded from this path.
        # -----------------------------------------------------------------------------
        fileUrl = os.path.join('ob', 'getfile')
        fileUrl = '/' + fileUrl + '/'

        # Concatenate the filename and url to form a full request.
        finalDownloadName = fileUrl + fileName

        # Download the data set.
        request_status = httpdl(OceanColorRetriever.BASE_URL,
                                finalDownloadName,
                                localpath=self._outputDirectory,
                                uncompress=True)

        # File was retrieved, or file was already present.
        if request_status == 0 or request_status == 200 \
                or request_status == 304:

            #print(os.path.join(os.getcwd(), self._outputDirectory, fileName))
            return(os.path.join(os.getcwd(), self._outputDirectory, fileName))

        # File not found (client error).
        else:

            msg = 'Client or server error: ' + str(request_status) + \
                '. ' + finalDownloadName

            raise RuntimeError(request_status, msg)

    # -------------------------------------------------------------------------
    # isValidDataSet
    # -------------------------------------------------------------------------
    @staticmethod
    def isValidDataSet(mission, dataset):
        if dataset in OceanColorRetriever.MISSION_DATASETS[mission]:
            return True
        else:
            return False

    # -------------------------------------------------------------------------
    # _validate
    # -------------------------------------------------------------------------
    def _validate(self, mission, dateTime):

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
