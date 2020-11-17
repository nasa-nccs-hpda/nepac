
import unittest

import pandas

from nepac.model.OceanColorRetriever import OceanColorRetriever

# -----------------------------------------------------------------------------
# class OceanColorRetrieverTestCase
#
# singularity shell -B /att /att/nobackup/iluser/containers/ilab-core-5.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_OceanColorRetriever
# -----------------------------------------------------------------------------
class OceanColorRetrieverTestCase(unittest.TestCase):

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):
            
            OceanColorRetriever('invalidMission', 'someDataSet')
        
        # Test valid mission and invalid data set.
        with self.assertRaisesRegex(RuntimeError, 'Invalid data set in:'):
            
            OceanColorRetriever('aqua', 'someDataSet')
        
        # Test valid everything.
        OceanColorRetriever('aqua', 'iPAR')
