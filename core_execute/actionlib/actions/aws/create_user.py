"""Create or update IAM users and attach inline assume-role policies."""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class CreateUserActionSpec(ActionSpec):
    """Parameters for creating or updating IAM users.

    Attributes:
      account: AWS account ID where users are managed.
      region: AWS region for IAM operations.
      user_names: List of IAM user names, or a template that renders to a CSV list.
      roles: List of role names/ARNs, or a template that renders to a CSV list.
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

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize user name inputs into a list.

        Accepts:
          - UserNames (list[str] or str)
          - UserName (single str)
        """
        if isinstance(values, dict):
            usernames = values.pop("user_names", None) or values.pop("UserNames", None) or []
            if isinstance(usernames, str):
                usernames = [usernames]
            username = values.pop("user_name", None) or values.pop("UserName", None)
            if username:
                usernames.append(username)
            values["user_names"] = usernames

        return values


class CreateUserActionResource(ActionResource):
    """Resource model for CreateUserAction (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and set canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::CreateUser"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, CreateUserActionSpec):
            values["spec"] = spec.model_dump()

        return values


class CreateUserAction(BaseAction[CreateUserActionSpec]):
    """Create or update IAM users and attach inline policies to assume roles.

    - Creates users that don't exist; skips creation for existing users
    - Builds or updates a single inline policy per user to allow sts:AssumeRole
    - Records results in action state and outputs
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the action and validate parameters.

        Args:
          definition: Action resource with metadata/spec.
          context: Rendering context for templates.
          deployment_details: Deployment metadata for this run.

        """
        super().__init__(definition, context, deployment_details)

        # Validate the parameters
        self.spec = CreateUserActionSpec(**definition.spec)

    def _resolve(self):
        """Render templates for account, region, user_names, and roles.

        - Supports list and string inputs
        - When strings are provided, treats them as comma-separated lists
        """
        log.trace("Resolving CreateUserAction")

        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.region = self.renderer.render_string(self.spec.region, self.context)

        if isinstance(self.spec.user_names, list):
            for i, user_name in enumerate(self.spec.user_names):
                self.spec.user_names[i] = self.renderer.render_string(user_name, self.context)
        elif isinstance(self.spec.user_names, str):
            names = self.renderer.render_string(self.spec.user_names, self.context)
            names = names.lstrip("[").rstrip("]").strip()
            names_list = []
            for name in names.split(","):
                name = name.lstrip("\"'").rstrip("\"'").strip()
                names_list.append(name)
            self.spec.user_names = names_list

        if isinstance(self.spec.roles, list):
            for i, role in enumerate(self.spec.roles):
                self.spec.roles[i] = self.renderer.render_string(role, self.context)
        elif isinstance(self.spec.roles, str):
            roles = self.renderer.render_string(self.spec.roles, self.context)
            roles = roles.lstrip("[").rstrip("]").strip()
            role_list = []
            for role in roles.split(","):
                role = role.lstrip("\"'").rstrip("\"'").strip()
                role_list.append(role)
            self.spec.roles = role_list

        log.trace("CreateUserAction resolved")

    def _execute(self):
        """Create or update IAM users and attach inline assume-role policies.

        Behavior:
          - Fails when no users are provided or IAM client cannot be created
          - Continues per user, recording created, skipped, and failed results
        """
        log.trace("Executing CreateUserAction")

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
                role=util.get_provisioning_role_arn(self.spec.account),
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
            if not self.spec.roles:
                log.warning("No roles specified for user '{}', skipping role attachment", user_name)
                continue

            if isinstance(self.spec.roles, str):
                self.spec.roles = [self.spec.roles]

            log.info("Creating and attaching inline policy for user '{}'", user_name)

            try:
                policy_name, policy_document = self._attach_inline_policy_to_user(iam_client, user_name, self.spec.roles)
                log.info("Successfully attached/updated role assumption policy for user '{}'", user_name)
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
                log.error("Unexpected error attaching/updating policy for user '{}': {}", user_name, e)
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

        log.trace("CreateUserAction execution completed")

    def _check(self):
        """Not applicable; IAM user operations are immediate."""
        log.trace("Checking CreateUserAction")

        self.set_complete("User put operations are immediate")

        log.trace("CreateUserAction check completed")

    def _unexecute(self):
        """No rollback; user creation/update cannot be undone automatically."""
        log.trace("Unexecuting CreateUserAction")

        self.set_complete("User put cannot be rolled back")

        log.trace("CreateUserAction unexecution completed")

    def _cancel(self):
        """No-op; user creation/update is immediate and cannot be cancelled."""
        log.trace("Cancelling CreateUserAction")

        self.set_complete("User put operations are immediate and cannot be cancelled")

        log.trace("CreateUserAction cancellation completed")

    def _check_user_exists(self, iam_client, user_name: str) -> bool:
        """Return True if the IAM user exists.

        Args:
          iam_client: IAM client.
          user_name: User name to check.

        Returns:
          True if user exists, else False.

        Raises:
          ClientError: For non-NoSuchEntity API errors.
        """
        try:
            iam_client.get_user(UserName=user_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                return False
            else:
                raise

    def _attach_inline_policy_to_user(self, iam_client, user_name: str, roles: list[str]) -> tuple[str, dict]:
        """Create or update an inline policy that allows assuming specified roles.

        Updates existing policy by replacing only sts:AssumeRole resources; preserves other statements.

        Args:
          iam_client: IAM client.
          user_name: Target IAM user name.
          roles: Role names or ARNs to allow assumption.

        Returns:
          Tuple (policy_name, policy_document).

        Raises:
          ClientError: If setting the inline policy fails.
          ValueError: If account ID is required to build role ARNs and is missing.
        """
        # Create policy name
        policy_name = f"{user_name}-AssumeRoles-Policy"

        # Convert roles to ARNs
        new_role_arns = set()
        for role in roles:
            if role.startswith("arn:aws:iam::"):
                new_role_arns.add(role)
            else:
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

                if isinstance(existing_policy_doc, str):
                    existing_policy = util.from_json(existing_policy_doc)
                else:
                    existing_policy = existing_policy_doc

                log.debug("Found existing policy for user '{}'", user_name)
                log.debug("Existing policy: {}", util.to_json(existing_policy))

            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchEntity":
                    log.debug("No existing policy found for user '{}', will create new one", user_name)
                    existing_policy = None
                else:
                    raise

            # Create the final policy document
            if existing_policy:
                policy_document = self._replace_assume_role_resources(existing_policy, new_role_arns)
            else:
                policy_document = self._create_policy_with_role_arns(new_role_arns)

            log.debug("Final policy document: {}", util.to_json(policy_document))
            log.info(
                "Setting inline policy '{}' for user '{}' with {} role(s)",
                policy_name,
                user_name,
                len(new_role_arns),
            )

            iam_client.put_user_policy(
                UserName=user_name,
                PolicyName=policy_name,
                PolicyDocument=util.to_json(policy_document),
            )

            log.info("Successfully set inline policy '{}' for user '{}'", policy_name, user_name)

            return policy_name, policy_document

        except ClientError as e:
            log.error("Failed to set inline policy '{}' for user '{}': {}", policy_name, user_name, e)
            raise

    def _replace_assume_role_resources(self, existing_policy: dict, new_role_arns: set) -> dict:
        """Replace sts:AssumeRole resources in an existing policy.

        Preserves all non-AssumeRole statements.

        Args:
          existing_policy: Existing policy document.
          new_role_arns: Set of role ARNs to place in AssumeRole Resource.

        Returns:
          Updated policy document.
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

            if isinstance(action, str):
                actions = [action]
            else:
                actions = action if isinstance(action, list) else []

            if "sts:AssumeRole" in actions and statement.get("Effect") == "Allow":
                updated_statement = statement.copy()
                updated_statement["Resource"] = sorted(list(new_role_arns))
                updated_policy["Statement"].append(updated_statement)
                assume_role_statement_found = True
                log.debug("Replaced sts:AssumeRole resources in existing statement")
            else:
                updated_policy["Statement"].append(statement)
                log.debug("Preserved non-AssumeRole statement: {}", statement.get("Effect", "Unknown"))

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
        """Create a simple policy document that allows assuming the given roles.

        Args:
          role_arns: Set of role ARNs to include.

        Returns:
          IAM policy document dict.
        """
        if not role_arns:
            return {"Version": "2012-10-17", "Statement": []}

        sorted_role_arns = sorted(list(role_arns))

        statement = {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": sorted_role_arns,
        }

        policy_document = {"Version": "2012-10-17", "Statement": [statement]}

        return policy_document

    def _create_inline_policy_document(self, roles: list[str]) -> dict:
        """Create an inline policy document for the provided role names.

        Args:
          roles: List of role names.

        Returns:
          IAM policy document dict.
        """
        role_arns = set()
        for role in roles:
            role_arn = f"arn:aws:iam::{self.spec.account}:role/{role}"
            role_arns.add(role_arn)

        return self._create_policy_with_role_arns(role_arns)

    @classmethod
    def generate_action_resource(cls, **kwargs) -> CreateUserActionResource:
        """Factory: create a typed CreateUserActionResource."""
        return CreateUserActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> CreateUserActionSpec:
        """Factory: create typed CreateUserActionSpec."""
        return CreateUserActionSpec(**kwargs)
