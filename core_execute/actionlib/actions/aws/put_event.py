from typing import Any

import core_logging as log

from core_framework.models import ActionDefinition, DeploymentDetails

from core_execute.actionlib.action import BaseAction

from core_db.event.models import EventModel


class PutEventAction(BaseAction):

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)
        self.type = self.params.Type or "STATUS"
        self.status = self.params.Status
        self.message = self.params.Message or ""
        self.identity = self.params.Identity
        self.item_type = deployment_details.Scope

    def _execute(self):
        try:
            t = self.type.upper()
            if t == "STATUS":
                log.status(self.status, self.message, identity=self.identity)
            elif t == "DEBUG":
                log.debug(self.message, identity=self.identity)
            elif t == "INFO":
                log.info(self.message, identity=self.identity)
            elif t == "WARN":
                log.warn(self.message, identity=self.identity)
            elif t == "ERROR":
                log.error(self.message, identity=self.identity)
            else:
                raise ValueError(
                    f"Invalid event type: {t}.  Must be one of: STATUS, DEBUG, INFO, WARN, ERROR"
                )

            event = EventModel(
                self.identity,
                event_type=self.type,
                item_type=self.item_type,
                status=self.status,
                message=self.message,
            )
            event.save()
        except Exception as e:
            log.error("Failed to save event to database: {}", e)
            self.set_failed("Failed to save event to database")
            return

        self.set_complete("Success")

    def _check(self):
        self.set_failed("Internal error - _check() should not have been called")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.type = self.renderer.render_string(self.type, self.context)
        self.status = self.renderer.render_string(self.status, self.context)
        self.message = self.renderer.render_string(self.message, self.context)
        self.identity = self.renderer.render_string(self.identity, self.context)
