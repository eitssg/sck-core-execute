"""Create or update IAM users in an AWS account."""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

import core_helper.aws as aws

import core_framework as util

from core_execute.actionlib.action import BaseAction


class PutUserActionSpec(ActionSpec):
    """Parameters for creating/updating IAM users.

    Attributes:
      account: AWS account ID where users will be managed.
      region: AWS region for IAM operations.
      user_names: List of user names, or a Jinja2 string that renders to a list.
      roles: List of role names (or Jinja2 string) users can assume.
    """

    user_names: list[str] | str = Field(
        ...,
        alias="UserNames",
        description="The list of users to create/update or a Jinja2 pattern that renders to a list",
    )
    roles: list[str] | str = Field(
        default_factory=list,
        alias="Roles",
        description="Roles to allow users to assume (list or Jinja2 pattern)",
    )


class PutUserActionResource(ActionResource):
    """Resource model for PutUserAction.

    Normalizes inputs and forces kind to 'AWS::PutUser'.
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::PutUser"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, PutUserActionSpec):
            values["spec"] = spec.model_dump()

        return values


class PutUserAction(BaseAction[PutUserActionSpec]):
    """Create or update IAM users and attach inline assume-role policies.

    - Creates users that do not exist; skips existing users
    - Attaches/updates an inline policy allowing sts:AssumeRole to specified roles
    - Records results and final policies in action state/outputs
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize action and validate parameters.

        Args:
          definition: Action resource with metadata/spec.
          context: Rendering context (variables used by templates).
          deployment_details: Portfolio/app/branch/build metadata.

        """
        super().__init__(definition, context, deployment_details)

        # Validate the parameters
        self.spec = PutUserActionSpec.model_validate(definition.spec)

    def _resolve(self):
        """Render template variables in parameters (account, region, users, roles)."""
        log.trace("Resolving PutUserAction")

        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.region = self.renderer.render_string(self.spec.region, self.context)

        if isinstance(self.spec.user_names, list):
            for i, user_name in enumerate(self.spec.user_names):
                # If user_names is a list, render each item as a Jinja2 template
                self.spec.user_names[i] = self.renderer.render_string(user_name, self.context)
        elif isinstance(self.spec.user_names, str):
            # If user_names is a string, render it as a Jinja2 template
            names = self.renderer.render_string(self.spec.user_names, self.context)

            # if the user has accidently put [ and ] at the beginning and end of the string,  remove them
            names = names.lstrip("[").rstrip("]").strip()

            # Split the names by comma and strip any surrounding quotes
            names_list = []
            for name in names.split(","):
                # We assume the response is formatted as a comma-separated list and if each value is surround with quotes, remove them
                name = name.lstrip("\"'").rstrip("\"'").strip()
                names_list.append(name)
            self.spec.user_names = names_list

        if isinstance(self.spec.roles, list):
            for i, role in enumerate(self.spec.roles):
                # If roles is a list, render each item as a Jinja2 template
                self.spec.roles[i] = self.renderer.render_string(role, self.context)
        elif isinstance(self.spec.roles, str):

            # If roles is a string, render it as a Jinja2 template
            roles = self.renderer.render_string(self.spec.roles, self.context)

            # if the user has accidently put [ and ] at the beginning and end of the string,  remove them
            roles = roles.lstrip("[").rstrip("]").strip()

            role_list = []
            for role in roles.split(","):
                # We assume the response is formatted as a comma-separated list and if each value is surround with quotes, remove them
                role = role.lstrip("\"'").rstrip("\"'").strip()
                role_list.append(role)
            self.spec.roles = role_list

        log.trace("PutUserAction resolved")

    def _execute(self):
        """Create/update IAM users and attach inline policies.

        Steps:
          1) Validate inputs
          2) Create users if missing
          3) Attach or update inline policy to allow sts:AssumeRole on roles
          4) Record results, final policies, and completion status
        """
        log.trace("Executing PutUserAction")

        # Validate required parameters
        if not self.spec.user_names:
            self.set_failed("UserNames parameter is required and must contain at least one user")
            log.error("UserNames parameter is required and must contain at least one user")
            return

        # Set initial state information
        self.set_state("Account", self.spec.account)
        self.set_state("Region", self.spec.region)
        self.set_state("UserNames", self.spec.user_names)
        self.set_state("AssignedRoles", self.spec.roles)  # KEEP ONLY THIS ONE
        self.set_state("PutStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("Account", self.spec.account)
        self.set_output("Region", self.spec.region)
        self.set_output("UserNames", self.spec.user_names)
        self.set_output("AssignedRoles", self.spec.roles)  # KEEP ONLY THIS ONE
        self.set_output("PutStarted", True)

        # Obtain an IAM client
        try:
            iam_client = aws.iam_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
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
        for user_name in self.spec.user_names:
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
                    error_code, error_message = self.parse_client_error(e)
                    failed_users.append(
                        {
                            "UserName": user_name,
                            "ErrorCode": error_code,
                            "ErrorMessage": error_message,
                            "Operation": "CreateUser",
                        }
                    )
                    continue
            else:
                log.info("User '{}' already exists, skipping creation", user_name)
                skipped_users.append(user_name)

            # Attach policies to the user
            if not self.spec.roles:
                log.warning(
                    "No roles specified for user '{}', skipping role attachment",
                    user_name,
                )
                continue

            # Ensure roles is a list
            if isinstance(self.spec.roles, str):
                self.spec.roles = [self.spec.roles]

            log.info("Creating and attaching inline policy for user '{}'", user_name)

            try:
                # Create and attach inline policy that allows assuming the specified roles
                policy_name, policy_document = self._attach_inline_policy_to_user(iam_client, user_name, self.spec.roles)
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
                error_code, error_message = self.parse_client_error(e)
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
            failure_details = [f"{user['UserName']} ({user['Operation']})" for user in failed_users]
            self.set_failed(f"Failed operations for users: {', '.join(failure_details)}")
        else:
            self.set_state("CreationResult", "SUCCESS")
            self.set_output("CreationResult", "SUCCESS")

            if skipped_users and not created_users:
                self.set_complete(f"All {len(skipped_users)} users already existed")
            else:
                self.set_complete(
                    f"Successfully processed {len(self.spec.user_names)} users: {len(created_users)} created, {len(skipped_users)} skipped"
                )

        log.trace("PutUserAction execution completed")

    def _check(self):
        """Confirm the operation is complete (IAM user puts are immediate)."""
        log.trace("Checking PutUserAction")

        # IAM user put is immediate, so if we get here, it's already complete
        self.set_complete("User put operations are immediate")

        log.trace("PutUserAction check completed")

    def _unexecute(self):
        """No rollback for user creation/update; report completion."""
        log.trace("Unexecuting PutUserAction")

        # User put cannot be undone

        self.set_complete("User put cannot be rolled back")

        log.trace("PutUserAction unexecution completed")

    def _cancel(self):
        """No-op; user operations are immediate and cannot be cancelled."""
        log.trace("Cancelling PutUserAction")

        # User put is immediate and cannot be cancelled
        self.set_complete("User put operations are immediate and cannot be cancelled")

        log.trace("PutUserAction cancellation completed")

    def _check_user_exists(self, iam_client, user_name: str) -> bool:
        """Return True if the IAM user exists.

        Args:
          iam_client: Boto3 IAM client.
          user_name: User name to check.

        Returns:
          True if user exists, else False.

        Raises:
          ClientError: For non-NotFound API errors.
        """
        try:
            iam_client.get_user(UserName=user_name)
            return True
        except ClientError as e:
            error_code, _ = self.parse_client_error(e)
            if error_code == "NoSuchEntity":
                return False

            raise

    def _attach_inline_policy_to_user(self, iam_client, user_name: str, roles: list[str]) -> tuple[str, dict]:
        """Create or update the user's inline assume-role policy.

        Replaces only the sts:AssumeRole resources, preserving other statements.

        Args:
          iam_client: Boto3 IAM client.
          user_name: Target user name.
          roles: Roles (names or ARNs) to allow assuming.

        Returns:
          Tuple of (policy_name, policy_document).

        Raises:
          ClientError: If the policy update fails.
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
                if not self.spec.account:
                    raise ValueError("Account ID is required to create role ARNs")
                role_arn = f"arn:aws:iam::{self.spec.account}:role/{role}"
                new_role_arns.add(role_arn)

        log.debug("Processing inline policy '{}' for user '{}'", policy_name, user_name)
        log.debug("Replacing sts:AssumeRole resources with: {}", list(new_role_arns))

        try:
            # Try to get existing policy first
            existing_policy = None
            try:
                response = iam_client.get_user_policy(UserName=user_name, PolicyName=policy_name)
                existing_policy_doc = response["PolicyDocument"]

                # Parse the URL-decoded policy document
                if isinstance(existing_policy_doc, str):
                    existing_policy = util.from_json(existing_policy_doc)
                else:
                    existing_policy = existing_policy_doc

                log.debug("Found existing policy for user '{}'", user_name)
                log.debug("Existing policy: {}", util.to_json(existing_policy))

            except ClientError as e:
                if "Error" in e.response and "Code" in e.response["Error"]:
                    if e.response["Error"]["Code"] == "NoSuchEntity":
                        log.debug(
                            "No existing policy found for user '{}', will create new one",
                            user_name,
                        )
                        existing_policy = None
                    else:
                        raise
                else:
                    raise

            # Create the final policy document
            if existing_policy:
                # Update existing policy by replacing sts:AssumeRole resources
                policy_document = self._replace_assume_role_resources(existing_policy, new_role_arns)
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

    def _replace_assume_role_resources(self, existing_policy: dict, new_role_arns: set) -> dict:
        """Replace resources in sts:AssumeRole statements, keep other statements.

        Args:
          existing_policy: Current IAM policy document.
          new_role_arms: Set of role ARNs to use.

        Returns:
          Updated IAM policy document.
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
        """Build a minimal policy that allows sts:AssumeRole on given ARNs.

        Args:
          role_arns: Set of role ARNs.

        Returns:
          IAM policy document.
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
        """Build an inline policy that allows assuming the given role names.

        Args:
          roles: Role names (converted to ARNs using params.account).

        Returns:
          IAM policy document.
        """
        # Convert role names to ARNs
        role_arns = set()
        for role in roles:
            role_arn = f"arn:aws:iam::{self.spec.account}:role/{role}"
            role_arns.add(role_arn)

        return self._create_policy_with_role_arns(role_arns)

    @classmethod
    def generate_action_resource(cls, **kwargs) -> PutUserActionResource:
        """Factory: create a typed PutUserActionResource."""
        return PutUserActionResource.model_validate(kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> PutUserActionSpec:
        """Factory: create typed PutUserActionSpec."""
        return PutUserActionSpec.model_validate(kwargs)
