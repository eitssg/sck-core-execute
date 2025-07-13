# Core-Execute

The execute is a module responsible for executing functios as defined by the "&lt;task&gt;.actions"
file which contains Action Definitions.

Action Defintions are subroutines that can be added to the deploytment lifecycle.

## Description

Actions are defined in the module folder ``core_execute/actionlib/actions**``

The name of the action is ``FOLDER::FOLDER::ActionNameAction`` where **ActionName** is the PascalCase for the python script which is in snake_case.

Here is an example:

Assume we have created a python script using the ActionDefintion API to grant access to KMS keys in the python script ``create_grants.py``

We then organize the module library and place the script ``create_grants.py`` into the subfolder ``core_execute/actionlib/actions/aws/kms``

Since the file/script is now in the file ``core_execute/actionlib/actions/aws/kms/create_grants.py`` we will derrive the name of the action for the ActionSpec API by:

1. Capetalize the folder names after the ``/actionslib/actions`` folder separated with `::`.  Example: ``AWS::KMS::``
2. Use PascalCase notation of the python script filename.  Example: ``CreateGrants``
3. To create the class append the word *Action* to the name.  Example:  ``CreateGrantsAction``

The name of the action is then, ``AWS::KMS::CreateGrants`` with a class name of ``CreateGrantsAction`` in the file ``aws/kms/create_grants.py``


## ActionSpec API

### The Action

In the file ``create_grants.py`` there will be a class ``CreateGrantsAction`` which will define the interface from ``BaseAction``


```python
class CreateGrantsAction(BaseAction):
    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)
```

When this class is instantated by the engine, it will pass the ``ActionSpec`` and ``DeploymentDetails`` to the ``__init__()`` function for use in the action script.

### The Action Definition

The action can be used by placing the action defintion with all required parameters into task actions defintion file ``<task>.actions`` such as a ``deployspec.yaml``

Example of an Action Definition:

```yaml
action-definition-label:
    type: AWS::KMS::CreateGrants
    depends_on: [ 'label of another action in this file' ]
    params:
        account: "The account to use for the action (required)"
        region: "The region to create the stack in (required)"
        kms_key_id: "The ID of the KMS key to create grants for (optionally required)"
        kms_key_arn: "The ARN of the KMS key to create grants for (optionally required)",
        grantee_principals: ["The principals to grant access to (required)"]
        operations: ["The operations to grant access for (required)"]
        ignore_failed_grants: False
    scope: "build"
```

The values of the ``params`` field are for th euse of the action class definition and can be any value as recognized by the action script.
