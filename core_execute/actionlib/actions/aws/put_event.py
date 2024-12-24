"""Record an event in the database"""

from typing import Any

import core_logging as log

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

from core_execute.actionlib.action import BaseAction

from core_db.event.actions import EventActions


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::GetStackReferences",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Type="The type of event to put (required) defaults to 'STATUS'",
            Status="The status of the event (required)",
            Message="The message to associate with the event (optional) defaults to ''",
            Identity="The identity of the event (optional)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class PutEventAction(BaseAction):
    """Record an event in the database

    This action will record an event in the database.  The event will be associated with the deployment details and the identity of the event.

    Attributes:
        Type: Use the value: ``AWS::PutEvent``
        Params.Type: The type of event to put (required) defaults to 'STATUS'
        Params.Status: The status of the event (required)
        Params.Message: The message to associate with the event (optional) defaults to ''
        Params.Identity: The identity of the event (optional)

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-putevent-label
              Type: "AWS::PutEvent"
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
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        if self.params.Type is None:
            self.params.Type = "STATUS"

        self.item_type = deployment_details.Scope

    def _execute(self):

        log.trace("PutEventAction._execute()")

        try:
            t = self.params.Type.upper()
            if t == "STATUS":
                log.status(
                    self.params.Status,
                    self.params.Message,
                    identity=self.params.Identity,
                )
            elif t == "DEBUG":
                log.debug(self.params.Message, identity=self.params.Identity)
            elif t == "INFO":
                log.info(self.params.Message, identity=self.params.Identity)
            elif t == "WARN":
                log.warn(self.params.Message, identity=self.params.Identity)
            elif t == "ERROR":
                log.error(self.params.Message, identity=self.params.Identity)
            else:
                log.fatal("Invalid event type: {}", t)
                raise ValueError(
                    f"Invalid event type: {t}.  Must be one of: STATUS, DEBUG, INFO, WARN, ERROR"
                )

            event = EventActions.create(
                self.params.Identity,
                event_type=self.params.Type,
                item_type=self.item_type,
                status=self.params.Status,
                message=self.params.Message,
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

        self.params.Type = self.renderer.render_string(self.params.Type, self.context)
        self.params.Status = self.renderer.render_string(
            self.params.Status, self.context
        )
        self.params.Message = self.renderer.render_string(
            self.params.Message, self.context
        )
        self.params.Identity = self.renderer.render_string(
            self.params.Identity, self.context
        )

        log.trace("PutEventAction._resolve() complete")
