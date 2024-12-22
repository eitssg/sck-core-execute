import core_logging as log

import core_helper.aws as aws

import core_execute.envinfo as envinfo
from core_execute.actionlib.action import BaseAction

import re


class CreateGrantsAction(BaseAction):
    """Create grants for a KMS key"""

    def __init__(self, definition, context, deployment_details):
        super().__init__(definition, context, deployment_details)

        self.account = self.params["Account"]
        self.region = self.params["Region"]
        self.kms_key_id = self.params.get("KmsKeyId", self.params.get("KmsKeyArn"))
        self.grantee_principals = self.params["GranteePrincipals"]
        self.operations = self.params["Operations"]
        self.ignore_failed_grants = self.params.get("IgnoreFailedGrants", True)

    def _execute(self):
        # Obtain an EC2 client
        kms_client = aws.kms_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        # Create the grants
        self.set_running(
            "Creating grants for KMS key '{}' in account '{}'".format(
                self.kms_key_id, self.account
            )
        )
        for principal in self.grantee_principals:
            # Use the grantee principal as the grant name, with unsupported characters replaced with '-'
            name = re.sub(r"[^a-zA-Z0-9:/_-]", "-", principal)

            try:
                params = {
                    "GranteePrincipal": principal,
                    "KeyId": self.kms_key_id,
                    "Name": name,
                    "Operations": self.operations,
                }

                log.debug("Creating grant", details=params)

                response = kms_client.create_grant(**params)

                log.debug(
                    "Grant creation was successful",
                    details={
                        "GrantId": response["GrantId"],
                        "GrantToken": response["GrantToken"],
                    },
                )
            except Exception as e:
                if self.ignore_failed_grants:
                    log.trace(
                        "Failed to create grant, but configured to ignore - {}", e
                    )
                else:
                    log.error("Failed to create grant - {}", e)
                    self.set_failed("Failed to create grant, principal may not exist")

        self.set_complete()

    def _check(self):
        self.set_failed("Internal error - _check() should not have been called")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
        self.kms_key_id = self.renderer.render_string(self.kms_key_id, self.context)
        self.grantee_principals = self.renderer.render_object(
            self.grantee_principals, self.context
        )
        self.ignore_failed_grants = self.renderer.render_string(
            self.ignore_failed_grants, self.context
        )
