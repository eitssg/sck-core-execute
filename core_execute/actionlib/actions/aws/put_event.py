"""Record an event in the database"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import ActionSpec, DeploymentDetails

from core_execute.actionlib.action import BaseAction

from core_db.event.actions import EventActions


class PutEventActionParams(BaseModel):
    """Parameters for the PutEventAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    type: str = Field(
        "STATUS",
        alias="Type",
        description="The type of event to put (required) defaults to 'STATUS'",
    )
    status: str = Field(
        ...,
        alias="Status",
        description="The status of the event (required)",
    )
    message: str = Field(
        "",
        alias="Message",
        description="The message to associate with the event (optional) defaults to ''",
    )
    identity: str = Field(
        None,
        alias="Identity",
        description="The identity of the event (optional)",
    )


class PutEventActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the PutEventActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-putevent-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::PutEvent"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "Type": None,
                "Status": "",
                "Message": "",
                "Identity": None,
            }

        return values


class PutEventAction(BaseAction):
    """Record an event in the database

    This action will record an event in the database.  The event will be associated with the deployment details and the identity of the event.

    Attributes:
        Kind: Use the value: ``AWS::PutEvent``
        Params.Type: The type of event to put (required) defaults to 'STATUS'
        Params.Status: The status of the event (required)
        Params.Message: The message to associate with the event (optional) defaults to ''
        Params.Identity: The identity of the event (optional)

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-putevent-name
              Kind: "AWS::PutEvent"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                Type: "STATUS"
                Status: "DEPLOY_SUCCESS"
                Message: "The deployment was successful"
                Identity: "prn:stack-portfolio:my-stack-app:my-stack-dev-branch:ver.10"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = PutEventActionParams(**definition.params)

        self.item_type = deployment_details.scope

    def _execute(self):

        log.trace("PutEventAction._execute()")

        try:
            t = self.params.type.upper()
            if t == "STATUS":
                log.status(
                    self.params.status,
                    self.params.message,
                    identity=self.params.identity,
                )
            elif t == "DEBUG":
                log.debug(self.params.message, identity=self.params.identity)
            elif t == "INFO":
                log.info(self.params.message, identity=self.params.identity)
            elif t == "WARN":
                log.warn(self.params.message, identity=self.params.identity)
            elif t == "ERROR":
                log.error(self.params.message, identity=self.params.identity)
            else:
                log.fatal("Invalid event type: {}", t)
                raise ValueError(
                    f"Invalid event type: {t}.  Must be one of: STATUS, DEBUG, INFO, WARN, ERROR"
                )

            event = EventActions.create(
                self.params.identity,
                event_type=self.params.type,
                item_type=self.item_type,
                status=self.params.status,
                message=self.params.message,
            )
            log.debug("Event created: {}", event)

        except Exception as e:
            log.error("Failed to save event to database: {}", e)
            self.set_failed("Failed to save event to database")
            return

        self.set_complete("Success")

        log.trace("PutEventAction._execute() complete")

    def _check(self):

        log.trace("PutEventAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("PutEventAction._check() complete")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("PutEventAction._resolve()")

        self.params.type = self.renderer.render_string(self.params.type, self.context)
        self.params.status = self.renderer.render_string(
            self.params.status, self.context
        )
        self.params.message = self.renderer.render_string(
            self.params.message, self.context
        )
        self.params.identity = self.renderer.render_string(
            self.params.identity, self.context
        )

        log.trace("PutEventAction._resolve() complete")
