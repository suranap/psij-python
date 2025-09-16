from packaging.version import Version
from psij.descriptor import Descriptor

__PSI_J_EXECUTORS__ = [
    Descriptor(
        name='nersc',
        nice_name='NERSC',
        version=Version('0.1.0'),
        cls='psij.executors.nersc.NERSCExecutor'
    )
]
