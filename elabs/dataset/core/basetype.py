
MAGIC = b'elabs123'
MAGIC_SIZE = len(MAGIC)
RWLOCK_DATA_SIZE = 200 # RWLock.RWLOCK_DATA_SIZE 
DATAFILE_HEAD_SIZE = len(MAGIC) + RWLOCK_DATA_SIZE + 8 
FIELD_LATEST_OFFSET = DATAFILE_HEAD_SIZE - 8 

BITWIDE = 64 
ValueBytes = int( BITWIDE / 8 )
MinutesPerDay = 24 * 60
