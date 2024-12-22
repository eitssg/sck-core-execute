import core_framework as util
from core_framework.constants import CORE_AUTOMATION_PIPELINE_PROVISIONING_ROLE


def provisioning_role_arn(account: str) -> str:

    scope_prefix = util.get_automation_scope()

    return "arn:aws:iam::{}:role/{}{}".format(
        account, scope_prefix, CORE_AUTOMATION_PIPELINE_PROVISIONING_ROLE
    )
