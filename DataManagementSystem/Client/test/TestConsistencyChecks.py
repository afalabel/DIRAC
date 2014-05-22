import unittest
from mock import Mock
from DIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

class ConsistencyChecksTestCase(unittest.TestCase):
  """ Base class for the Consistency Checks test cases
  """
  def setUp( self ):
    self.transClientMock = Mock()
    self.transClientMock.getFileDescendants.return_value = {'OK': True,
                                                         'Value': {'Failed': [],
                                                                   'NotProcessed': [],
                                                                   'Successful': {'aa.raw': ['bb.raw', 'bb.log']},
                                                                   'WithMetadata': {'aa.raw': {'bb.raw': {'FileType': 'RAW',
                                                                                                          'RunNumber': 97019,
                                                                                                          'GotReplica':'Yes'},
                                                                                               'bb.log': {'FileType': 'LOG',
                                                                                                          'GotReplica':'Yes'}
                                                                                               }
                                                                                    }
                                                                   }
                                                         }
    self.transClientMock.getFileMetadata.return_value = {'OK': True,
                                                      'Value': {'aa.raw': {'FileType': 'RAW',
                                                                           'RunNumber': 97019},
                                                                'bb.raw': {'FileType': 'RAW',
                                                                           'RunNumber': 97019},
                                                                'dd.raw': {'FileType': 'RAW',
                                                                           'RunNumber': 97019},
                                                                'bb.log': {'FileType': 'LOG'},
                                                                '/bb/pippo/aa.dst':{'FileType': 'DST'},
                                                                '/lhcb/1_2_1.Semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'},
                                                                '/lhcb/1_1.semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}
                                                                }
                                                      }

    self.dmMock = Mock()
    self.dmMock.getReplicas.return_value = {'OK': True, 'Value':{'Successful':{'bb.raw':'metadataPippo'},
                                                                  'Failed':{}}}
    
#     >>> res
# {'OK': True, 'Value': {'Successful': {'/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw': {'CERN-RAW': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw', 'CNAF-RAW': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/tape/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw'}, '/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw': {'CERN-RAW': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw', 'CNAF-RAW': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/tape/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw'}}, 'Failed': {}}}
# >>> from pprint import pprint
# >>> pprint.pprint(res)
# Traceback (most recent call last):
#   File "<stdin>", line 1, in <module>
# AttributeError: 'function' object has no attribute 'pprint'
# >>> pprint(res)
# {'OK': True,
#  'Value': {'Failed': {},
#            'Successful': {'/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw': {'CERN-RAW': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw',
#                                                                                                      'CNAF-RAW': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/tape/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw'},
#                           '/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw': {'CERN-RAW': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw',
#                                                                                                      'CNAF-RAW': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/tape/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw'}}}}

    
    self.dmMock.getCatalogFileMetadata( lfnChunk )
    
#     
#     >>> pprint(res)
# {'OK': True,
#  'Value': {'Failed': {},
#            'Successful': {'/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw': {'CERN-RAW': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw',
#                                                                                                      'CNAF-RAW': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/tape/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw'},
#                           '/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw': {'CERN-RAW': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw',
#                                                                                                      'CNAF-RAW': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/tape/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw'}}}}
# >>> res = rm.getCatalogFileMetadata(["/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw","/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw"])
# >>> pprint(res)
# {'OK': True,
#  'Value': {'Failed': {},
#            'Successful': {'/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw': {'CheckSumType': 'AD',
#                                                                                                      'Checksum': '379aa433',
#                                                                                                      'CreationDate': datetime.datetime(2012, 11, 18, 12, 58, 13),
#                                                                                                      'GUID': '1d22c7c8-3175-11e2-866d-00259011312c',
#                                                                                                      'Mode': 436,
#                                                                                                      'ModificationDate': datetime.datetime(2012, 11, 18, 12, 58, 13),
#                                                                                                      'NumberOfLinks': 1,
#                                                                                                      'Size': 3145732036,
#                                                                                                      'Status': '-'},
#                           '/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw': {'CheckSumType': 'AD',
#                                                                                                      'Checksum': '91324a09',
#                                                                                                      'CreationDate': datetime.datetime(2012, 11, 18, 12, 58, 24),
#                                                                                                      'GUID': '3bfbb8da-3175-11e2-866e-00259011312c',
#                                                                                                      'Mode': 436,
#                                                                                                      'ModificationDate': datetime.datetime(2012, 11, 18, 12, 58, 24),
#                                                                                                      'NumberOfLinks': 1,
#                                                                                                      'Size': 3145788944,
#                                                                                                      'Status': '-'}}}}


    self.cc = ConsistencyChecks( transClient = Mock(), dm = self.dmMock )
    self.cc.prod = 9999
    self.cc.checkFC2SE()
    
    
  def tearDown( self ):
    pass
  

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ConsistencyChecksTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
