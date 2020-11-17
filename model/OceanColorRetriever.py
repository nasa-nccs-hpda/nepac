
from collections import Iterable
import datetime

import pandas


# -----------------------------------------------------------------------------
# class OceanColorRetriever
#
# https://oceandata.sci.gsfc.nasa.gov
# https://oceancolor.gsfc.nasa.gov/data/download_methods/#api
# -----------------------------------------------------------------------------
class OceanColorRetriever(object):

    BASE_URL = 'https://oceandata.sci.gsfc.nasa.gov'
    
    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_DATASETS = {
        
        'aqua' : ['iPAR', 'Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_412', 'Rrs_443',
                  'Rrs_469', 'Rrs_488', 'Rrs_531', 'Rrs_547', 'Rrs_555',
                  'Rrs_645', 'Rrs_667', 'Rrs_678'],
                  
        'czcs' : ['Kd_490', 'Rrs_520', 'Rrs_543', 'Rrs_550', 'Rrs_670'],
        
        'goci' : ['Kd_490', 'POC', 'Rrs_412', 'Rrs_443', 'Rrs_490', 'Rrs_555',
                  'Rrs_660', 'Rrs_680'],
                  
        'hico' : ['Kd_490', 'PIC', 'POC', 'Rrs_353', 'Rsf_358', 'Rrs_364',
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
                  
        'octs' : ['Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_412', 'Rrs_443',
                  'Rrs_490', 'Rrs_516', 'Rrs_565', 'Rrs_667'],
                  
        'seawifs' : ['Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_412', 'Rrs_443',
                     'Rrs_490', 'Rrs_555', 'Rrs_670'],
        
        'terra': ['iPAR', 'Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_412', 'Rrs_443',
                  'Rrs_469', 'Rrs_488', 'Rrs_531', 'Rrs_547', 'Rrs_555',
                  'Rrs_645', 'Rrs_667', 'Rrs_678'],
                  
        'viirs' : ['Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_411', 'Rrs_445',
                     'Rrs_489', 'Rrs_556', 'Rrs_667'],

        'viirsj1' : ['Kd_490', 'PAR', 'PIC', 'POC', 'Rrs_411', 'Rrs_445',
                     'Rrs_489', 'Rrs_556', 'Rrs_667'],
    }
    
    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_DATES = {
        
        'aqua' : pandas.date_range('2002-07-04', datetime.date.today()),
        'czcs' : pandas.date_range('1978-10-30', '1986-06-22'),
        'goci' : pandas.date_range('2010-06-01', datetime.date.today()),
        'hico' : pandas.date_range('2009-09-25', '2014-09-13'),
        'octs' : pandas.date_range('1996-11-01', '1997-06-30'),
        'seawifs' : pandas.date_range('1997-09-04', '2010-12-11'),
        'terra': pandas.date_range('2000-02-24', datetime.date.today()),
        'viirs' : pandas.date_range('2017-11-29', datetime.date.today()),
        'viirsj1' : pandas.date_range('2012-01-02', datetime.date.today())
    }
    
    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_FILE_SUFFIXES = {
        
        'aqua' : 'OC',
        'czcs' : '',
        'goci' : '',
        'hico' : '',
        'octs' : '',
        'seawifs' : '',
        'terra': 'OC',
        'viirs' : '',
        'viirsj1' : '')
    }

    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_LEVEL = 2    

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self, mission, dataSet):

        self._validate(mission, dataSet, dateRange)
        
        self._mission = mission
        self._dataSets = dataSets
        
    # -------------------------------------------------------------------------
    # run
    # -------------------------------------------------------------------------
    def run(self):
        
        # Form the file name.
        
        # Download the data set.
        
    # -------------------------------------------------------------------------
    # _validate
    # -------------------------------------------------------------------------
    def _validate(self, mission, dataSets, dateRange):
        
        # Validate mission.
        if not mission in OceanColorRetriever.MISSION_DATASETS:
            
            msg = 'Invalid mission: ' + str(mission) + \
                  '.  Valid missions: ' + \
                  str(OceanColorRetriever.MISSION_DATASETS.keys())
                  
            raise RuntimeError(msg)

        # Validate data sets.
        if not ds in OceanColorRetriever.MISSION_DATASETS[mission]:
            
            msg = 'Invalid data set: ' + str(dataSet) + \
                  '.  Valid missions: ' + \
                  str(OceanColorRetriever.MISSION_DATASETS[mission])
                  
            raise RuntimeError(msg)
            