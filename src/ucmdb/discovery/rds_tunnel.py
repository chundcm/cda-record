# coding=utf-8
import string
import re

import logger
import modeling
import scp

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    # # Write implementation to return new result CIs here...
    service_context = Framework.getDestinationAttribute('SERVICE_CONTEXT')
    service_type = Framework.getDestinationAttribute('SERVICE_TYPE')
    database_root_class = Framework.getDestinationAttribute('DATABASE_CLASS')
    database_id = Framework.getDestinationAttribute('DATABASE_ID')
    scp_id = Framework.getDestinationAttribute('SCP_ID')

    databaseOsh = modeling.createOshByCmdbIdString(database_root_class, database_id)
    scpOsh = scp.createOshById('scp', scp_id)
    OSHVResult.add(databaseOsh)
    OSHVResult.add(scpOsh)

    OSHVResult.addAll(scp.createOwnerShip(scp_id, databaseOsh))
    return OSHVResult