##
# Copyright (c) 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from jplus.compiler.environment import metamagic as mm_jplus
from jplus.compiler.environment.metamagic import (
    Language, RuntimeLanguage, JPlusWebRuntime, JPlusWebRuntimeAdapter,
    JPlusDynamicRuntimeDerivative
)

# install jplus environment
mm_jplus.Environment().activate()
