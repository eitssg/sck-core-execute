from typing import Dict, Any, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

from core_framework.models import DeploymentDetails
import core_logging as log
from core_renderer import Jinja2Renderer
from pydantic import Field, model_validator
from core_execute.actionlib.action import BaseAction, ActionResource, ActionSpec


class SendEmailActionSpec(ActionSpec):
    """Parameters for SendEmailAction.

    parameters:   {
        "to_email": "user@example.com",
        "subject": "Email Subject",
        "template_type": "forgot_password|password_updated|welcome",
        "template_data": {
            "auth_code": "12345678",
            "user_name": "John",
            "ip_address": "192.168.1.1",
            "user_agent": "Mozilla/5.0..."
        }

    the 'template_data' depends on the 'template_type' parameter.
    You should consider this the Jinja2 template context and you can
    specify whatever variables you want to replace in the template.

    See the Documentation for the template_type and available template_data parameters.
    """

    to_email: str = Field(..., description="Email recipient address", alias="ToEmail")
    cc: Optional[str] = Field(None, description="CC email address", alias="CcEmail")
    bcc: Optional[str] = Field(None, description="BCC email address", alias="BccEmail")
    subject: str = Field(..., description="Email subject", alias="Subject")
    template_type: str = Field(..., description="Type of email template", alias="TemplateType")
    template_data: Dict[str, Any] = Field(
        default_factory=dict, description="Data to populate the email template", alias="TemplateData"
    )


class SendEmailActionResource(ActionResource):
    """ActionResource wrapper for SendEmailAction with defaults."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Set SendEmail-specific defaults."""

        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "SYSTEM::SendEmail"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, SendEmailActionSpec):
            values["spec"] = spec.model_dump()

        return values


class SendEmailAction(BaseAction):
    """Send email using SMTP configuration with template rendering."""

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize SendEmailAction with validated parameters."""
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.spec = SendEmailActionSpec(**definition.spec)

        # Initialize template renderer
        template_dir = os.path.join(os.path.dirname(__file__), "templates")
        self.template_renderer = Jinja2Renderer(template_dir)

        # Store rendered email content
        self.template_html = None
        self.template_txt = None

        # ✅ Track if this action has been executed this Step Function run
        self.executed_this_run = False

    def can_initialize(self) -> bool:
        """Check if action can be reinitialized for rerun.

        Email actions can always be reinitialized for user-initiated reruns.
        """
        return True

    def initialize(self) -> bool:
        """Initialize/reset action for rerun.

        Resets the action completely, clearing all previous state and allowing
        it to be executed fresh in the new Step Function run.

        Returns:
            bool: True if initialization was successful, False otherwise.

        """
        log.debug("Initializing SendEmailAction {} for rerun", self.name)

        # Call super initialize to clear status and outputs
        super().initialize()

        status = self.get_status()

        if self.is_failed():
            log.debug("Action {} was in a failed state ({}), resetting to pending", self.name, status)
            self.set_pending("Initialized")
        else:  # pending or complete or running.  If it was running, we need to run 'check' to update status
            log.debug("Action {} is in state ({}), leaving status unchanged", self.name, status)

        log.debug("SendEmailAction {} initialized", self.name)

        return True

    def can_execute(self) -> bool:
        """Check if action can execute.

        Email actions can execute if:
        1. Standard conditions are met
        2. Haven't been executed this Step Function run yet
        3. Required parameters are present
        """
        try:
            # ✅ Check if already executed this Step Function run
            if self.executed_this_run:
                log.debug("SendEmailAction {} already executed this Step Function run", self.name)
                return False

            # Call super condition checking
            if not super().can_execute():
                return False

            # Check email-specific requirements
            if not self.spec.to_email:
                log.debug("SendEmailAction {} missing required to_email parameter", self.name)
                return False

            if not self.spec.template_type:
                log.debug("SendEmailAction {} missing required template_type parameter", self.name)
                return False

            return True

        except Exception as e:
            log.error("Error checking if SendEmailAction {} can execute: {}", self.name, e)
            return False

    def _resolve(self):
        """Perform Jinja2 template rendering using template_data context."""
        log.trace("Resolving SendEmailAction templates")

        try:
            template_name = self.spec.template_type
            email_context = self.spec.template_data

            # Render HTML template
            try:
                self.template_html = self.template_renderer.render_file(f"{template_name}.html.j2", email_context)
                log.debug("HTML template rendered successfully: {}.html.j2", template_name)
            except Exception as e:
                log.warning("Failed to render HTML template '{}.html.j2': {}", template_name, e)
                self.template_html = None

            # Render text template
            try:
                self.template_txt = self.template_renderer.render_file(f"{template_name}.txt.j2", email_context)
                log.debug("Text template rendered successfully: {}.txt.j2", template_name)
            except Exception as e:
                log.warning("Failed to render text template '{}.txt.j2': {}", template_name, e)
                self.template_txt = None

            # Check if at least one template was rendered
            if not self.template_html and not self.template_txt:
                log.error("No templates could be rendered for template_type: {}", template_name)
                self.set_failed(f"No templates available for template_type: {template_name}")
                return

            # Update template usage statistics for the send_email application
            template_count_key = f"templates/{template_name}/usage_count"
            current_count = self.get_state(template_count_key) or 0
            self.set_state(template_count_key, current_count + 1)

            # Track template types used by this application
            templates_used_key = "templates/types_used"
            templates_used = self.get_state(templates_used_key) or []
            if template_name not in templates_used:
                templates_used.append(template_name)
                self.set_state(templates_used_key, templates_used)

            # Output application-level statistics (no identity prefix)
            self.set_output(f"template_{template_name}_count", current_count + 1)
            self.set_output("templates_used", templates_used)

            log.debug("Email templates resolved successfully for: {}", template_name)

        except Exception as e:
            log.error("Failed to resolve email templates: {}", e)
            self.set_failed(f"Template resolution failed: {e}")

        log.trace("SendEmailAction template resolution completed")

    def _execute(self):
        """Execute email sending operation."""
        log.trace("Executing SendEmailAction")

        # ✅ Mark as executed this Step Function run immediately
        self.executed_this_run = True
        execution_marker_key = f"{self.state_namespace}/executed_this_run"
        self.set_state("executed_this_run", True)

        try:
            # Validate required parameters
            if not self.spec.to_email:
                log.error("No recipient email specified")
                self.set_failed("No recipient email specified")
                return

            # Check if templates were rendered
            if not self.template_html and not self.template_txt:
                log.error("No email content available - templates not rendered")
                self.set_failed("No email content available - template rendering failed")
                return

            # Send email
            success = self._send_email(
                to_email=self.spec.to_email,
                subject=self.spec.subject,
                html_content=self.template_html or "",
                text_content=self.template_txt or "",
            )

            if success:
                # Update send statistics for the application
                template_name = self.spec.template_type
                sent_count_key = f"templates/{template_name}/sent_count"
                current_sent = self.get_state(sent_count_key) or 0
                self.set_state(sent_count_key, current_sent + 1)

                # Track total emails sent by application
                total_sent_key = "application/total_emails_sent"
                total_sent = self.get_state(total_sent_key) or 0
                self.set_state(total_sent_key, total_sent + 1)

                # Output application-level statistics
                self.set_output(f"template_{template_name}_sent", current_sent + 1)
                self.set_output("total_emails_sent", total_sent + 1)

                log.info(
                    "Email sent successfully",
                    details={
                        "template_type": self.spec.template_type,
                        "subject": self.spec.subject,
                        "sent_count": current_sent + 1,
                        "executed_this_run": True,  # ✅ Log execution tracking
                    },
                )

                self.set_complete("Email sent successfully")

            else:
                # Track failed sends for the application
                template_name = self.spec.template_type
                failed_count_key = f"templates/{template_name}/failed_count"
                current_failed = self.get_state(failed_count_key) or 0
                self.set_state(failed_count_key, current_failed + 1)

                # Track total failures
                total_failed_key = "application/total_emails_failed"
                total_failed = self.get_state(total_failed_key) or 0
                self.set_state(total_failed_key, total_failed + 1)

                # Output failure statistics
                self.set_output(f"template_{template_name}_failed", current_failed + 1)
                self.set_output("total_emails_failed", total_failed + 1)

                self.set_failed("Email sending failed")

        except Exception as e:
            log.error("Email sending action failed: {}", e)
            self.set_failed(f"Email sending action failed: {e}")

        log.trace("SendEmailAction execution completed")

    def _send_email(self, to_email: str, subject: str, html_content: str, text_content: str) -> bool:
        """Send email using SMTP configuration."""
        log.trace("Sending email to recipient")

        try:
            # Get SMTP configuration from environment
            smtp_server = os.getenv("SMTP_SERVER", "localhost")
            smtp_port = int(os.getenv("SMTP_PORT", "587"))
            smtp_username = os.getenv("SMTP_USERNAME")
            smtp_password = os.getenv("SMTP_PASSWORD")
            from_email = os.getenv("FROM_EMAIL", smtp_username)
            use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"

            if not smtp_username or not smtp_password:
                log.warning("SMTP credentials not configured - email sending disabled")
                return False

            if not from_email:
                log.error("FROM_EMAIL not configured")
                return False

            # Create message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = from_email
            msg["To"] = to_email
            if self.spec.cc:
                msg["Cc"] = self.spec.cc
            if self.spec.bcc:
                msg["Bcc"] = self.spec.bcc

            # Add content parts (only add parts that have content)
            if text_content:
                text_part = MIMEText(text_content, "plain", "utf-8")
                msg.attach(text_part)

            if html_content:
                html_part = MIMEText(html_content, "html", "utf-8")
                msg.attach(html_part)

            # Send email
            log.debug("Connecting to SMTP server: {}:{}", smtp_server, smtp_port)

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                if use_tls:
                    server.starttls()
                    log.debug("SMTP TLS enabled")

                server.login(smtp_username, smtp_password)
                log.debug("SMTP authentication successful")

                server.send_message(msg)
                log.debug("Email sent successfully via SMTP")

            return True

        except smtplib.SMTPAuthenticationError as e:
            log.error("SMTP authentication failed: {}", e)
            return False
        except smtplib.SMTPRecipientsRefused as e:
            log.error("SMTP recipients refused: {}", e)
            return False
        except smtplib.SMTPServerDisconnected as e:
            log.error("SMTP server disconnected: {}", e)
            return False
        except Exception as e:
            log.error("SMTP email sending failed: {}", e)
            return False

    def _check(self):
        """Check email sending status (not needed for synchronous email sending)."""
        self.set_complete("Email sending is synchronous and assumed complete")

    def _cancel(self):
        """Cancel email sending operation (not applicable for synchronous sending)."""
        self.set_complete("Email sending operation cancelled")

    def _unexecute(self):
        """Rollback email sending (not possible to unsend email)."""
        self.set_complete("Email sending cannot be rolled back")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> SendEmailActionResource:
        """Generate ActionResource for SendEmailAction."""
        return SendEmailActionResource(**kwargs)

    @classmethod
    def generate_action_spec(cls, **kwargs) -> SendEmailActionSpec:
        """Generate ActionSpec for SendEmailAction."""
        return SendEmailActionSpec(**kwargs)
