"""Share an AMI image with other AWS accounts by granting launch permissions"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator, field_validator

import core_logging as log

from core_framework.models import ActionSpec, DeploymentDetails

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class ShareImageActionParams(BaseModel):
    """Parameters for the ShareImageAction.

    Contains all configuration needed to share an AMI image with other AWS accounts
    by granting launch permissions to specified target accounts.

    Attributes
    ----------
    account : str
        The AWS account ID where the source AMI image is located
    region : str
        The AWS region where the source AMI image is located
    image_name : str
        The name of the AMI image to share (used to locate the image)
    accounts_to_share : list[str]
        List of AWS account IDs to grant launch permissions to
    siblings : list[str]
        List of AWS account IDs that are permitted as sharing targets
        Used for validation to ensure sharing is only done to approved accounts
    tags : dict[str, str], optional
        Additional tags to apply to the image (default: empty dict)

    Examples
    --------
    Basic image sharing configuration::

        params = ShareImageActionParams(
            account="123456789012",
            region="us-east-1",
            image_name="my-application-v1.0.0",
            accounts_to_share=["234567890123", "345678901234"],
            siblings=["234567890123", "345678901234", "456789012345"]
        )
    """

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(..., alias="Account", description="The account where the source AMI image is located")
    region: str = Field(..., alias="Region", description="The region where the source AMI image is located")
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
        """Validate that accounts_to_share contains valid AWS account IDs.

        Parameters
        ----------
        v : list[str]
            List of account IDs to validate

        Returns
        -------
        list[str]
            The validated list of account IDs

        Raises
        ------
        ValueError
            If the list is empty or contains invalid account IDs
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
        """Validate that siblings contains valid AWS account IDs.

        Parameters
        ----------
        v : list[str]
            List of sibling account IDs to validate

        Returns
        -------
        list[str]
            The validated list of sibling account IDs

        Raises
        ------
        ValueError
            If the list contains invalid account IDs
        """
        for account_id in v:
            if not account_id.isdigit() or len(account_id) != 12:
                raise ValueError(f"Invalid AWS sibling account ID: {account_id}. Must be 12 digits.")

        return v

    @model_validator(mode="after")
    def validate_sharing_permissions(self) -> "ShareImageActionParams":
        """Validate that all accounts_to_share are in the siblings list.

        Returns
        -------
        ShareImageActionParams
            The validated model instance

        Raises
        ------
        ValueError
            If any target account is not in the siblings list
        """
        for target_account in self.accounts_to_share:
            if target_account not in self.siblings:
                raise ValueError(
                    f"Target account {target_account} is not in the siblings list. "
                    f"Only sibling accounts are permitted as sharing targets."
                )

        return self


class ShareImageActionSpec(ActionSpec):
    """Generate the action definition for ShareImage.

    Provides a convenience wrapper for creating ShareImage actions
    with sensible defaults for common AMI sharing use cases.

    Examples
    --------
    Creating an image sharing action spec with defaults::

        spec = ShareImageActionSpec()
        # Results in action with name "share-image", kind "share_image"
        # and template-based default parameters
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate and set default parameters for the ShareImageActionSpec.

        Provides sensible defaults for action name, kind, scope, and
        basic parameter structure using template variables.

        Parameters
        ----------
        values : dict[str, Any]
            The input values dictionary

        Returns
        -------
        dict[str, Any]
            The values dictionary with defaults applied
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "share-image"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::ShareImage"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "Account": "{{ deployment.account }}",
                "Region": "{{ deployment.region }}",
                "ImageName": "{{ app.name }}-{{ branch.name }}-{{ build.version }}",
                "AccountsToShare": [],
                "Siblings": [],
                "Tags": {},
            }
        return values


class ShareImageAction(BaseAction):
    """Share an AMI image with other AWS accounts by granting launch permissions.

    This action modifies the launch permissions of an existing AMI image to allow
    specified AWS accounts to launch EC2 instances from the image. The action
    validates that target accounts are in the approved siblings list before sharing.

    **Key Features:**

    - Locate AMI by name and grant launch permissions to target accounts
    - Validate sharing targets against approved siblings list
    - Support for template variables in image names and account references
    - Comprehensive error handling for missing images and permission issues
    - State tracking for sharing operations and results

    **Use Cases:**

    - Share application AMIs across development, staging, and production accounts
    - Distribute base images to multiple business units or teams
    - Enable cross-account deployments with custom AMI images
    - Implement controlled AMI distribution workflows

    **Action Parameters:**

    :param Account: AWS account ID where the source AMI image is located
    :type Account: str
    :param Region: AWS region where the source AMI image is located
    :type Region: str
    :param ImageName: Name of the AMI image to share
    :type ImageName: str
    :param AccountsToShare: List of AWS account IDs to grant launch permissions to
    :type AccountsToShare: list[str]
    :param Siblings: List of AWS account IDs permitted as sharing targets
    :type Siblings: list[str]
    :param Tags: Additional tags to apply to the image
    :type Tags: dict[str, str]

    **Examples:**

    Simple AMI sharing to development accounts:

    .. code-block:: yaml

        - name: share-app-image
          kind: share_image
          params:
            Account: "{{ deployment.account }}"
            Region: "{{ deployment.region }}"
            ImageName: "{{ app.name }}-{{ branch.name }}-{{ build.version }}"
            AccountsToShare:
              - "123456789012"  # Development account
              - "234567890123"  # Staging account
            Siblings:
              - "123456789012"
              - "234567890123"
              - "345678901234"  # Production account

    Cross-region image sharing:

    .. code-block:: yaml

        - name: share-base-image
          kind: share_image
          params:
            Account: "{{ deployment.account }}"
            Region: "us-east-1"
            ImageName: "company-base-image-v2.1.0"
            AccountsToShare: "{{ sharing.target_accounts }}"
            Siblings: "{{ sharing.approved_accounts }}"
            Tags:
              SharedBy: "{{ deployment.identity }}"
              SharedAt: "{{ deployment.timestamp }}"

    **Security Considerations:**

    - Only accounts in the Siblings list can be targets for sharing
    - Image sharing grants launch permissions but not modification rights
    - Original account retains full control over the source AMI
    - Sharing can be revoked by modifying launch permissions

    **State Tracking:**

    This action tracks execution state:

    - ``image_id`` - The AMI ID that was shared
    - ``shared_accounts`` - List of accounts that received permissions
    - ``status`` - Success/error status of the sharing operation
    - ``error_message`` - Details of any errors encountered
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the ShareImageAction.

        Parameters
        ----------
        definition : ActionSpec
            The action specification containing parameters and configuration
        context : dict[str, Any]
            Template rendering context with deployment variables
        deployment_details : DeploymentDetails
            Deployment context and metadata
        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = ShareImageActionParams(**definition.params)

        # Add deployment tracking tag if available
        if deployment_details.delivered_by:
            self.params.tags["DeliveredBy"] = deployment_details.delivered_by

    def _execute(self):
        """Execute the AMI image sharing operation.

        Locates the specified AMI image by name and modifies its launch permissions
        to grant access to the specified target accounts. Validates that all target
        accounts are in the approved siblings list before sharing.

        The execution process:

        1. Creates EC2 client with appropriate IAM role
        2. Locates AMI image by name using describe_images API
        3. Validates target accounts against siblings list
        4. Modifies image launch permissions to add target accounts
        5. Records sharing results in action state

        Raises
        ------
        Exception
            If AMI lookup fails, validation fails, or permission modification fails

        Notes
        -----
        This method implements the core functionality and should not be
        called directly. Use the action execution framework instead.
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
        """Check operation - not applicable for AMI sharing.

        AMI sharing is an atomic operation that either succeeds or fails.
        There is no meaningful check operation for launch permission modifications.

        Raises
        ------
        RuntimeError
            Always raises as check operation is not supported
        """
        log.trace("ShareImageAction._check()")
        self.set_failed("Check operation not supported for AMI image sharing")
        log.trace("ShareImageAction._check() complete")

    def _unexecute(self):
        """Unexecute operation - revoke launch permissions.

        Removes the launch permissions that were granted during execution,
        effectively unsharing the AMI from the target accounts.

        Notes
        -----
        This operation attempts to revoke previously granted permissions.
        If the image no longer exists, the operation is considered successful.
        """
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
        """Cancel operation - not applicable for AMI sharing.

        AMI sharing operations are atomic and complete quickly.
        Cancellation is not supported for this action type.

        Notes
        -----
        This is a no-op method as AMI sharing operations cannot be cancelled.
        """
        log.debug("Cancel requested for AMI sharing - operation cannot be cancelled")

    def _resolve(self):
        """Resolve template variables and prepare parameters for execution.

        Renders all template variables in the action parameters using the
        provided context. This includes account IDs, region, image name,
        and any other templated values.

        **Template Variables Available:**

        - ``deployment.*`` - Deployment context (account, region, environment)
        - ``app.*`` - Application information (name, version, config)
        - ``branch.*`` - Branch details (name, type, commit)
        - ``build.*`` - Build information (version, number, artifacts)
        - ``env.*`` - Environment variables
        - Action outputs from dependencies

        Raises
        ------
        Exception
            If template rendering fails or parameter validation errors occur

        Notes
        -----
        This method prepares data for execution and should not be
        called directly. Use the action execution framework instead.
        """
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
