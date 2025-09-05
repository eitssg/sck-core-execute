"""Share an AMI with other AWS accounts by granting launch permissions.

Locates an AMI by name and updates its launch permissions for target accounts.
"""

from typing import Any
from pydantic import Field, model_validator, field_validator

import core_logging as log

from core_framework.models import ActionResource, DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class ShareImageActionSpec(ActionSpec):
    """Parameters for sharing an AMI image.

    Attributes:
      account: AWS account ID where the source AMI resides.
      region: AWS region of the source AMI.
      image_name: AMI name used to locate the image.
      accounts_to_share: Target AWS account IDs to grant permissions.
      siblings: Approved account IDs allowed as sharing targets.
      tags: Extra tags applied during processing (optional).
    """

    image_name: str = Field(..., alias="ImageName", description="The name of the AMI image to share")
    accounts_to_share: list[str] = Field(
        ...,
        alias="AccountsToShare",
        description="List of AWS account IDs to grant launch permissions to",
    )
    siblings: list[str] = Field(
        ...,
        alias="Siblings",
        description="List of AWS account IDs that are permitted as sharing targets",
    )
    tags: dict[str, str] = Field(
        default_factory=dict,
        alias="Tags",
        description="Additional tags to apply to the image",
    )

    @field_validator("accounts_to_share")
    @classmethod
    def validate_accounts_to_share(cls, v: list[str]) -> list[str]:
        """Validate target account IDs.

        Args:
          v: List of AWS account IDs.

        Returns:
          The validated list.

        Raises:
          ValueError: If empty or any ID is not exactly 12 digits.
        """
        if not v:
            raise ValueError("At least one account must be specified to share with")

        for account_id in v:
            if not account_id.isdigit() or len(account_id) != 12:
                raise ValueError(f"Invalid AWS account ID: {account_id}. Must be 12 digits.")

        return v

    @field_validator("siblings")
    @classmethod
    def validate_siblings(cls, v: list[str]) -> list[str]:
        """Validate approved sibling account IDs.

        Args:
          v: List of sibling AWS account IDs.

        Returns:
          The validated list.

        Raises:
          ValueError: If any ID is not exactly 12 digits.
        """
        for account_id in v:
            if not account_id.isdigit() or len(account_id) != 12:
                raise ValueError(f"Invalid AWS sibling account ID: {account_id}. Must be 12 digits.")

        return v

    @model_validator(mode="after")
    def validate_sharing_permissions(self) -> "ShareImageActionSpec":
        """Ensure all targets are in the approved siblings list.

        Returns:
          Self.

        Raises:
          ValueError: If any target is not approved.
        """
        for target_account in self.accounts_to_share:
            if target_account not in self.siblings:
                raise ValueError(
                    f"Target account {target_account} is not in the siblings list. "
                    f"Only sibling accounts are permitted as sharing targets."
                )

        return self


class ShareImageActionResource(ActionResource):
    """Resource model for ShareImage.

    Normalizes inputs and forces kind to 'AWS::ShareImage'.
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec.

        - Forces kind to 'AWS::ShareImage'
        - Accepts spec as dict or ShareImageActionSpec
        """
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::ShareImage"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, ShareImageActionSpec):
            values["spec"] = spec.model_dump()

        return values


class ShareImageAction(BaseAction):
    """Grant launch permissions on an AMI to other AWS accounts.

    Validates target accounts against the approved siblings list before sharing.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
        parent_action_name: str | None = None,
    ):
        """Initialize the action and validate parameters.

        Args:
          definition: Action resource with metadata/spec.
          context: Rendering context with deployment variables.
          deployment_details: Deployment metadata for tracking.
          parent_action_name: Optional parent action name.
        """
        super().__init__(definition, context, deployment_details, parent_action_name)

        # Validate the action parameters
        self.params = ShareImageActionSpec(**definition.spec)

        # Add deployment tracking tag if available
        if deployment_details.delivered_by:
            self.params.tags["DeliveredBy"] = deployment_details.delivered_by

    def _execute(self):
        """Share the AMI by updating launch permissions.

        Steps:
          1) Describe AMI by name
          2) Validate target accounts against siblings
          3) Modify image attribute (LaunchPermission Add)
          4) Record results in action state

        Raises:
          Exception: On lookup, validation, or permission errors.
        """
        log.trace("ShareImageAction._execute()")

        try:
            # Obtain an EC2 client
            ec2_client = aws.ec2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )

            log.debug(f"Finding AMI image with name '{self.params.image_name}'")

            # Find image by name
            response = ec2_client.describe_images(Filters=[{"Name": "name", "Values": [self.params.image_name]}])

            if len(response["Images"]) == 0:
                message = (
                    f"Could not find AMI image with name '{self.params.image_name}'. It may have been deleted or does not exist."
                )
                log.warning(message)
                self.set_state("status", "skipped")
                self.set_state("error_message", message)
                self.set_complete(message)
                return

            image_id = response["Images"][0]["ImageId"]
            log.debug(f"Found AMI image '{image_id}' with name '{self.params.image_name}'")

            # Validate that all target accounts are in siblings list
            invalid_accounts = [acc for acc in self.params.accounts_to_share if acc not in self.params.siblings]
            if invalid_accounts:
                message = f"Cannot share to accounts {invalid_accounts} - they are not in the approved siblings list"
                log.error(message)
                self.set_state("status", "error")
                self.set_state("error_message", message)
                self.set_failed(message)
                return

            # Modify image launch permissions
            ec2_client.modify_image_attribute(
                ImageId=image_id,
                LaunchPermission={"Add": [{"UserId": account_id} for account_id in self.params.accounts_to_share]},
            )

            # Record successful sharing
            self.set_state("image_id", image_id)
            self.set_state("shared_accounts", self.params.accounts_to_share)
            self.set_state("status", "success")

            success_message = f"Successfully shared AMI {image_id} to accounts {self.params.accounts_to_share}"
            log.info(success_message)
            self.set_complete(success_message)

        except Exception as e:
            error_message = f"Failed to share AMI image: {str(e)}"
            log.error(error_message)
            self.set_state("status", "error")
            self.set_state("error_message", error_message)
            self.set_failed(error_message)

        log.trace("ShareImageAction._execute() complete")

    def _check(self):
        """No check step for AMI sharing; mark as not supported."""
        log.trace("ShareImageAction._check()")
        self.set_failed("Check operation not supported for AMI image sharing")
        log.trace("ShareImageAction._check() complete")

    def _unexecute(self):
        """Revoke launch permissions granted during execution (best effort)."""
        log.trace("ShareImageAction._unexecute()")

        try:
            # Get the shared accounts and image ID from state
            shared_accounts = self.get_state("shared_accounts")
            image_id = self.get_state("image_id")

            if not shared_accounts or not image_id:
                log.debug("No sharing state found - nothing to unexecute")
                return

            # Obtain an EC2 client
            ec2_client = aws.ec2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )

            # Remove launch permissions
            ec2_client.modify_image_attribute(
                ImageId=image_id,
                LaunchPermission={"Remove": [{"UserId": account_id} for account_id in shared_accounts]},
            )

            log.info(f"Successfully revoked launch permissions for AMI {image_id} from accounts {shared_accounts}")

        except Exception as e:
            log.warning(f"Failed to revoke AMI launch permissions during unexecute: {str(e)}")
            # Don't fail the unexecute operation for permission issues

        log.trace("ShareImageAction._unexecute() complete")

    def _cancel(self):
        """No-op; AMI sharing is atomic and cannot be cancelled."""
        log.debug("Cancel requested for AMI sharing - operation cannot be cancelled")

    def _resolve(self):
        """Render templates and prepare parameters for execution."""
        log.trace("ShareImageAction._resolve()")

        try:
            # Render template variables
            self.params.account = self.renderer.render_string(self.params.account, self.context)
            self.params.region = self.renderer.render_string(self.params.region, self.context)
            self.params.image_name = self.renderer.render_string(self.params.image_name, self.context)

            # Render accounts_to_share list
            rendered_accounts = []
            for account in self.params.accounts_to_share:
                rendered_account = self.renderer.render_string(str(account), self.context)
                rendered_accounts.append(rendered_account)
            self.params.accounts_to_share = rendered_accounts

            # Render siblings list
            rendered_siblings = []
            for sibling in self.params.siblings:
                rendered_sibling = self.renderer.render_string(str(sibling), self.context)
                rendered_siblings.append(rendered_sibling)
            self.params.siblings = rendered_siblings

            # Render tags
            rendered_tags = {}
            for key, value in self.params.tags.items():
                rendered_key = self.renderer.render_string(str(key), self.context)
                rendered_value = self.renderer.render_string(str(value), self.context)
                rendered_tags[rendered_key] = rendered_value
            self.params.tags = rendered_tags

            log.debug(f"Resolved image sharing for '{self.params.image_name}' to accounts {self.params.accounts_to_share}")

        except Exception as e:
            error_message = f"Failed to resolve template variables: {str(e)}"
            log.error(error_message)
            self.set_failed(error_message)

        log.trace("ShareImageAction._resolve() complete")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> ShareImageActionResource:
        """Factory: create a typed ShareImageActionResource."""
        return ShareImageActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> ShareImageActionSpec:
        """Factory: create typed ShareImageActionSpec."""
