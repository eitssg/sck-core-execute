"""Create or update IAM users in an AWS account"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class PutUserActionParams(ActionParams):
    """
    Parameters for the PutUserAction.

    :param account: The AWS account ID where users will be created/updated (required)
    :type account: str
    :param region: The AWS region for IAM operations (required)
    :type region: str
    :param user_names: The list of users to create/update (required)
    :type user_names: list[str] | str
    :param roles: The list of roles to assign to the users (optional)
    :type roles: list[str] | str
    """

    user_names: list[str] | str = Field(
        ...,
        alias="UserNames",
        description="The list of users to create/update or a jinja2 pattern to create a list of users (required)",
    )
    roles: list[str] | str = Field(
        default_factory=list,
        alias="Roles",
        description="The list of roles to assign to the users or a jinja2 pattern to create a list of roles (optional)",
    )


class PutUserActionSpec(ActionSpec):
    """
    Generate the action definition for PutUserAction.

    This class provides default values and validation for PutUserAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the parameters for the PutUserActionSpec.

        :param values: Input values for validation
        :type values: dict[str, Any]
        :return: Validated and potentially modified values
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-putuser-name"  # FIXED
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::PutUser"
        if not values.get(
            "depends_on", values.get("DependsOn")
        ):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Spec")):
            values["params"] = {
                "account": "",
                "region": "",
                "user_names": [],
                "roles": [],
            }
        return values


class PutUserAction(BaseAction):
    """
    Create or update IAM users in an AWS account.

    This action will create new IAM users or update existing ones in an AWS account.
    For each user, it will create an inline policy that allows assuming the specified roles.
    If a user already exists, only the role assignments will be updated.

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::PutUser``
    :Spec.Account: The account where the users are located (required)
    :Spec.Region: The region for the IAM operations (required)
    :Spec.UserNames: The list of user names to create/update (required)

    .. rubric:: ActionSpec Example

    .. code-block:: yaml

        - Name: action-aws-putuser-name  # FIXED
          Kind: "AWS::PutUser"
          Spec:
            Account: "154798051514"
            Region: "us-east-1"
            UserNames: ["john.smith", "jane.doe"]
            Roles: ["admin", "developer"]
          Scope: "build"

    .. note::
        If users already exist, only their role assignments will be updated.

    .. warning::
        Users created by this action will have permissions to assume the specified roles.
        Ensure role permissions are appropriate for the users being created.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the parameters
        self.params = PutUserActionParams(**definition.params)

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving PutUserAction")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )

        if isinstance(self.params.user_names, list):
            for i, user_name in enumerate(self.params.user_names):
                # If user_names is a list, render each item as a Jinja2 template
                self.params.user_names[i] = self.renderer.render_string(
                    user_name, self.context
                )
        elif isinstance(self.params.user_names, str):
            # If user_names is a string, render it as a Jinja2 template
            names = self.renderer.render_string(self.params.user_names, self.context)

            # if the user has accidently put [ and ] at the beginning and end of the string,  remove them
            names = names.lstrip("[").rstrip("]").strip()

            # Split the names by comma and strip any surrounding quotes
            names_list = []
            for name in names.split(","):
                # We assume the response is formatted as a comma-separated list and if each value is surround with quotes, remove them
                name = name.lstrip("\"'").rstrip("\"'").strip()
                names_list.append(name)
            self.params.user_names = names_list

        if isinstance(self.params.roles, list):
            for i, role in enumerate(self.params.roles):
                # If roles is a list, render each item as a Jinja2 template
                self.params.roles[i] = self.renderer.render_string(role, self.context)
        elif isinstance(self.params.roles, str):

            # If roles is a string, render it as a Jinja2 template
            roles = self.renderer.render_string(self.params.roles, self.context)

            # if the user has accidently put [ and ] at the beginning and end of the string,  remove them
            roles = roles.lstrip("[").rstrip("]").strip()

            role_list = []
            for role in roles.split(","):
                # We assume the response is formatted as a comma-separated list and if each value is surround with quotes, remove them
                role = role.lstrip("\"'").rstrip("\"'").strip()
                role_list.append(role)
            self.params.roles = role_list

        log.trace("PutUserAction resolved")

    def _execute(self):
        """
        Execute the user creation/update operation.

        This method creates new IAM users or updates existing ones with the specified roles.
        For each user, it will create/update an inline policy for role assumptions.
        IAM user operations are typically fast and don't require long-running monitoring.

        :raises: Sets action to failed if user creation/update fails
        """
        log.trace("Executing PutUserAction")

        # Validate required parameters
        if not self.params.user_names:
            self.set_failed(
                "UserNames parameter is required and must contain at least one user"
            )
            log.error(
                "UserNames parameter is required and must contain at least one user"
            )
            return

        # Set initial state information
        self.set_state("Account", self.params.account)
        self.set_state("Region", self.params.region)
        self.set_state("UserNames", self.params.user_names)
        self.set_state("AssignedRoles", self.params.roles)  # KEEP ONLY THIS ONE
        self.set_state("PutStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("Account", self.params.account)
        self.set_output("Region", self.params.region)
        self.set_output("UserNames", self.params.user_names)
        self.set_output("AssignedRoles", self.params.roles)  # KEEP ONLY THIS ONE
        self.set_output("PutStarted", True)

        # Obtain an IAM client
        try:
            iam_client = aws.iam_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create IAM client: {}", e)
            self.set_failed(f"Failed to create IAM client: {e}")
            return

        # Track put results
        created_users = []
        failed_users = []
        skipped_users = []
        users_with_policies = []
        final_policies = {}  # ADD THIS - Track final policies per user

        # Process each user
        for user_name in self.params.user_names:
            log.info("Processing user '{}'", user_name)

            # create the user
            if not user_name:
                log.error("User name cannot be empty")
                self.set_failed("User name cannot be empty")
                return

            # Check if the user already exists
            if not self._check_user_exists(iam_client, user_name):
                log.info("Creating user '{}'", user_name)

                try:
                    iam_client.create_user(UserName=user_name)
                    created_users.append(user_name)
                    log.info("User '{}' created successfully", user_name)
                except ClientError as e:
                    log.error("Failed to create user '{}': {}", user_name, e)
                    failed_users.append(
                        {
                            "UserName": user_name,
                            "ErrorCode": e.response["Error"]["Code"],
                            "ErrorMessage": e.response["Error"]["Message"],
                            "Operation": "CreateUser",
                        }
                    )
                    continue
            else:
                log.info("User '{}' already exists, skipping creation", user_name)
                skipped_users.append(user_name)

            # Attach policies to the user
            if not self.params.roles:
                log.warning(
                    "No roles specified for user '{}', skipping role attachment",
                    user_name,
                )
                continue

            # Ensure roles is a list
            if isinstance(self.params.roles, str):
                self.params.roles = [self.params.roles]

            log.info("Creating and attaching inline policy for user '{}'", user_name)

            try:
                # Create and attach inline policy that allows assuming the specified roles
                policy_name, policy_document = self._attach_inline_policy_to_user(
                    iam_client, user_name, self.params.roles
                )
                log.info(
                    "Successfully attached/updated role assumption policy for user '{}'",
                    user_name,
                )
                users_with_policies.append(user_name)

                # ADD THIS - Store the final policy for this user
                final_policies[user_name] = {
                    "PolicyName": policy_name,
                    "PolicyDocument": policy_document,
                }

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                error_message = e.response["Error"]["Message"]
                log.error(
                    "Failed to attach/update role assumption policy for user '{}': {} - {}",
                    user_name,
                    error_code,
                    error_message,
                )
                failed_users.append(
                    {
                        "UserName": user_name,
                        "ErrorCode": error_code,
                        "ErrorMessage": error_message,
                        "Operation": "AttachInlinePolicy",
                    }
                )
                continue
            except Exception as e:
                log.error(
                    "Unexpected error attaching/updating policy for user '{}': {}",
                    user_name,
                    e,
                )
                failed_users.append(
                    {
                        "UserName": user_name,
                        "ErrorCode": "UnexpectedError",
                        "ErrorMessage": str(e),
                        "Operation": "AttachInlinePolicy",
                    }
                )
                continue

        # Set completion state (update near the end of _execute)
        self.set_state("CreationCompleted", True)
        self.set_state("CompletionTime", util.get_current_timestamp())
        self.set_state("CreatedUsers", created_users)
        self.set_state("FailedUsers", failed_users)
        self.set_state("SkippedUsers", skipped_users)
        self.set_state("UsersWithPolicies", users_with_policies)
        # REMOVE THIS LINE: self.set_state("AssignedRoles", self.params.roles)
        self.set_state("FinalPolicies", final_policies)

        # Set outputs
        self.set_output("CreationCompleted", True)
        self.set_output("CreatedUsers", created_users)
        self.set_output("FailedUsers", failed_users)
        self.set_output("SkippedUsers", skipped_users)
        self.set_output("UsersWithPolicies", users_with_policies)
        # REMOVE THIS LINE: self.set_output("AssignedRoles", self.params.roles)
        self.set_output("FinalPolicies", final_policies)

        # Determine overall result
        if failed_users:
            self.set_state("CreationResult", "PARTIAL_FAILURE")
            self.set_output("CreationResult", "PARTIAL_FAILURE")
            failure_details = [
                f"{user['UserName']} ({user['Operation']})" for user in failed_users
            ]
            self.set_failed(
                f"Failed operations for users: {', '.join(failure_details)}"
            )
        else:
            self.set_state("CreationResult", "SUCCESS")
            self.set_output("CreationResult", "SUCCESS")

            if skipped_users and not created_users:
                self.set_complete(f"All {len(skipped_users)} users already existed")
            else:
                self.set_complete(
                    f"Successfully processed {len(self.params.user_names)} users: {len(created_users)} created, {len(skipped_users)} skipped"
                )

        log.trace("PutUserAction execution completed")

    def _check(self):
        """
        Check the status of the user creation/update operation.

        IAM user operations are typically immediate, so this method confirms completion.
        """
        log.trace("Checking PutUserAction")

        # IAM user put is immediate, so if we get here, it's already complete
        self.set_complete("User put operations are immediate")

        log.trace("PutUserAction check completed")

    def _unexecute(self):
        """
        Rollback the user creation/update operation.

        .. note::
            User creation cannot be automatically rolled back. Created users and their
            policies remain in place. Manual cleanup may be required.
        """
        log.trace("Unexecuting PutUserAction")

        # User put cannot be undone

        self.set_complete("User put cannot be rolled back")

        log.trace("PutUserAction unexecution completed")

    def _cancel(self):
        """
        Cancel the user creation/update operation.

        .. note::
            User creation/update operations are immediate and cannot be cancelled once started.
        """
        log.trace("Cancelling PutUserAction")

        # User put is immediate and cannot be cancelled
        self.set_complete("User put operations are immediate and cannot be cancelled")

        log.trace("PutUserAction cancellation completed")

    def _check_user_exists(self, iam_client, user_name: str) -> bool:
        """
        Check if a user exists in IAM.

        :param iam_client: IAM client
        :type iam_client: boto3.client
        :param user_name: Name of the user to check
        :type user_name: str
        :return: True if user exists, False otherwise
        :rtype: bool
        """
        try:
            iam_client.get_user(UserName=user_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                return False
            else:
                # Re-raise other errors
                raise

    def _attach_inline_policy_to_user(
        self, iam_client, user_name: str, roles: list[str]
    ) -> tuple[str, dict]:
        """
        Create and attach an inline policy to a user that allows assuming specified roles.
        If the policy already exists, replace only the sts:AssumeRole resources with the new ones,
        preserving any other policy statements.

        :param iam_client: IAM client
        :type iam_client: boto3.client
        :param user_name: Name of the user
        :type user_name: str
        :param roles: List of role names to allow assumption
        :type roles: list[str]
        :return: Tuple of (policy_name, policy_document)
        :rtype: tuple[str, dict]
        :raises ClientError: If policy attachment fails
        """

        # Create policy name
        policy_name = f"{user_name}-AssumeRoles-Policy"

        # Convert roles to ARNs
        new_role_arns = set()
        for role in roles:
            # check if the role is already an ARN or just a name
            if role.startswith("arn:aws:iam::"):
                new_role_arns.add(role)
            else:
                # If it's just a role name, convert it to ARN format
                if not self.params.account:
                    raise ValueError("Account ID is required to create role ARNs")
                role_arn = f"arn:aws:iam::{self.params.account}:role/{role}"
                new_role_arns.add(role_arn)

        log.debug("Processing inline policy '{}' for user '{}'", policy_name, user_name)
        log.debug("Replacing sts:AssumeRole resources with: {}", list(new_role_arns))

        try:
            # Try to get existing policy first
            existing_policy = None
            try:
                response = iam_client.get_user_policy(
                    UserName=user_name, PolicyName=policy_name
                )
                existing_policy_doc = response["PolicyDocument"]

                # Parse the URL-decoded policy document
                if isinstance(existing_policy_doc, str):
                    existing_policy = util.from_json(existing_policy_doc)
                else:
                    existing_policy = existing_policy_doc

                log.debug("Found existing policy for user '{}'", user_name)
                log.debug("Existing policy: {}", util.to_json(existing_policy))

            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchEntity":
                    log.debug(
                        "No existing policy found for user '{}', will create new one",
                        user_name,
                    )
                    existing_policy = None
                else:
                    raise

            # Create the final policy document
            if existing_policy:
                # Update existing policy by replacing sts:AssumeRole resources
                policy_document = self._replace_assume_role_resources(
                    existing_policy, new_role_arns
                )
            else:
                # Create new policy with just the assume role statement
                policy_document = self._create_policy_with_role_arns(new_role_arns)

            log.debug("Final policy document: {}", util.to_json(policy_document))
            log.info(
                "Setting inline policy '{}' for user '{}' with {} role(s)",
                policy_name,
                user_name,
                len(new_role_arns),
            )

            # Put/update the inline policy on the user (this replaces any existing policy)
            iam_client.put_user_policy(
                UserName=user_name,
                PolicyName=policy_name,
                PolicyDocument=util.to_json(policy_document),
            )

            log.info(
                "Successfully set inline policy '{}' for user '{}'",
                policy_name,
                user_name,
            )

            # CHANGE THIS - Return both policy name and document
            return policy_name, policy_document

        except ClientError as e:
            log.error(
                "Failed to set inline policy '{}' for user '{}': {}",
                policy_name,
                user_name,
                e,
            )
            raise

    def _replace_assume_role_resources(
        self, existing_policy: dict, new_role_arns: set
    ) -> dict:
        """
        Replace the resources in sts:AssumeRole statements with new role ARNs,
        while preserving all other policy statements.

        :param existing_policy: The existing IAM policy document
        :type existing_policy: dict
        :param new_role_arns: Set of new role ARNs to use
        :type new_role_arns: set
        :return: Updated policy document
        :rtype: dict
        """
        # Start with a copy of the existing policy
        updated_policy = {
            "Version": existing_policy.get("Version", "2012-10-17"),
            "Statement": [],
        }

        statements = existing_policy.get("Statement", [])
        assume_role_statement_found = False

        # Process each existing statement
        for statement in statements:
            action = statement.get("Action", [])

            # Handle both string and list actions
            if isinstance(action, str):
                actions = [action]
            else:
                actions = action if isinstance(action, list) else []

            # Check if this is an sts:AssumeRole statement
            if "sts:AssumeRole" in actions and statement.get("Effect") == "Allow":
                # Replace the resources in this statement
                updated_statement = statement.copy()
                updated_statement["Resource"] = sorted(list(new_role_arns))
                updated_policy["Statement"].append(updated_statement)
                assume_role_statement_found = True
                log.debug("Replaced sts:AssumeRole resources in existing statement")
            else:
                # Keep other statements unchanged
                updated_policy["Statement"].append(statement)
                log.debug(
                    "Preserved non-AssumeRole statement: {}",
                    statement.get("Effect", "Unknown"),
                )

        # If no sts:AssumeRole statement was found, add one
        if not assume_role_statement_found:
            new_statement = {
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": sorted(list(new_role_arns)),
            }
            updated_policy["Statement"].append(new_statement)
            log.debug("Added new sts:AssumeRole statement")

        return updated_policy

    def _create_policy_with_role_arns(self, role_arns: set) -> dict:
        """
        Create a policy document with the specified role ARNs.

        :param role_arns: Set of role ARNs to include in the policy
        :type role_arns: set
        :return: IAM policy document
        :rtype: dict
        """
        if not role_arns:
            # Return empty policy if no roles
            return {"Version": "2012-10-17", "Statement": []}

        # Sort role ARNs for consistent output
        sorted_role_arns = sorted(list(role_arns))

        # Create single statement with all role ARNs
        statement = {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": sorted_role_arns,
        }

        policy_document = {"Version": "2012-10-17", "Statement": [statement]}

        return policy_document

    def _create_inline_policy_document(self, roles: list[str]) -> dict:
        """
        Create an inline policy document that allows assuming the specified roles.

        :param roles: List of role names to allow assumption
        :type roles: list[str]
        :return: IAM policy document
        :rtype: dict
        """
        # Convert role names to ARNs
        role_arns = set()
        for role in roles:
            role_arn = f"arn:aws:iam::{self.params.account}:role/{role}"
            role_arns.add(role_arn)

        return self._create_policy_with_role_arns(role_arns)

    @classmethod
    def generate_action_spec(cls, **kwargs) -> PutUserActionSpec:
        return PutUserActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> PutUserActionParams:
        return PutUserActionParams(**kwargs)
