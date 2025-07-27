# Core-Execute

The execute is a module responsible for executing functios as defined by the "&lt;task&gt;.actions"
file which contains Action Definitions.

Action Defintions are subroutines that can be added to the deploytment lifecycle.


# Core-API

JSON API with FastAPI server

## Table of Contents

- [Overview](#core-automation-platform)
- [Layers and Components](#layers-and-components)
- [Getting Started](#getting-started)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Core Automation Platform

A completely opinionated and obnoxiously correct, even if in its own mind, model for forcing a behaviour in AWS.

## Layers and Components

### Layer 1 - Core Framework
          
**Core Framework**
The foundational models that provide tools and structures used by all layers. Standard YAML and JSON extensions

**Core Helper**
Tools that provide convenience functions for interacting with AWS or Local system

**Core Logging**
An enhanced python logging facility that provides a bit (little bit) easier interoperability between CloudWatch Logging and local Logging.  A couple of things added, like trace level logging.

**Core Renderer**
Jinja2 Rendering enhancements. Custom filters and round-trip rendering 

### Layer 2 - Core DB
The database integration system.  PynamoDB on DynamoDB at the moment. I'm considering taking the API and maybe use another Doc store.  This is where all the CMDB is managed.  Storing events (such as deployments) and items (Components) created and deployed for the Application.

### Layer 3 - Core Execute
Not only do we need to manage CloudFormation stacks, we also need to be able to perform standard action on AWS. This could be granting privileges, copying images to other accounts, adding and removing users, and, even deploying CloudFormation stacks with actions supporting tasks for plan, apply, deploy, teardown. Every action comes into Execute.  So, it processes a list of actions meaning it can run many operations.  As such it runs as an AWS Step-Function.

### Layer 4 - Core Report
I basically do nothing and don't know why I exist.  Maybe the logging that I do is my only purpose.  Core Runner calls me, but I don't know why it goes to the trouble of doing so.  I'll consider doing more in the future.

### Layer 5 - Core Runner
I have one and only one task.  I take your task payload, generate a unique execution ID (you can think of this as a correlation Id), log the fact you wish to perform a task to the database, and then call Core Execute to start the step-function going with your process ID.  That's all.  Or, maybe for some reason I call Core Report to log the tasks outcome.

### Layer 6 - Core Deployspec
A deployspec is simple.  Don't over complicate it.  It's just a list of actions. An action can operate ONLY on ONE AWS account and ONE region.  Now, I wish to take those actions and run them on hundreds of AWS accounts or in many regions.  And, if the task is to run 2 or 3 actions on 100 accounts, you can use me.  I will create 2 or 300 actions one for one account and AWS region combination.  I don't run them, I just upload these actions to an S3 artefacts folder, but, with one task command, you can kick them off all at once by calling Core Runner.

### Layer 7 - Core Component
I'm just a CloudFormation stack generator and an actions generator. When we call runner to deploy a stack, we need a stack template and a list of actions. That's what I do.  I create them.  You say "I want a web site". Our very smart engineers have created one 'component' that has combined the 50 AWS resources needed to make that web site component, and I generate a CloudFormation stack to deploy your web site component with all the tagging that you guys need to make sure it's safe and secure.  Actions will run after the stack is deployed ensuring everything else that needs to be tweaked is done.  I am the Component Compiler. Today, my component templates are carefully crafted.  In the future, I may switch to using AWS's Cloud Development Toolkit.  We'll see.  Exactly as the Core Deployspec system does, I upload these compiled CloudFormation templates and actions to S3 so you can kick them off with Core Runner.

### Layer 8 - Core Invoker
I am your traffic cop.  A task defines templates, actions, artefacts, etc. You give me a task, and I'll decide what to do with it.  If you need to run the DeploySpec processor, or the Component processor, or simply need to run your task's actions (one of compile, deploy, plan, apply, or teardown) with Core Runner, pass it to me, I'll decide.  You never need to call Core Runner or Core Execute or the Compiler processors yourself.  Just pass it to me, I'll take care of it.

### Layer 9 - Core Organization
Think of me as the tool you can use to manage the OU's in your Billing Account and create new AWS accounts and get them all setup and ready with the Roles that are necessary to allow the rest of this system to run in that account. This is...The Account Factory.  Today, I understand I'm rather antiquated and the engineers are using a separate program.  I hope they update me soon.

### Layer 10 - Core Codecommit
If you are using AWS Codecommit, you can push me out and I'll sit there as a trigger on one of your infrastructure repositories.  You have a /platform folder in your repository, and I'll know it.  You make an update, and I'll call Core Invoker to make it happen.

### Layer 11 - Core API
Everyone needs a JSON HTTP API these days. And, I've a couple of nice features. First, if you are running me in a docker container, I'll spin up a FastAPI server so you can GET and POST HTTP JSON requests to me.  If you want to run me in AWS API Gateway, no problem! Simply deploy my Core Automation Task and Template 'Core Component', and I'll be ready to accept requests from the AWS API Gateway and I'll already know about where the Core Invoker is running so we can start right away. Don't worry, you can always run me in a docker container and use me locally in the meantime.

### Layer 12 - Core CLI
Ah! The old days of where we started.  I'm a command line interface.  A CLI. I'm a wrapper and helpers to allow you to specify tasks and send them directly to the Core Invoker. I'll can compose a task and get it all ready to run. There is, however, one thing that I do need to do, and that's 'upload'.  It's the one thing I was built for.  You give me a command line, and I'll upload your templates and files to S3 so that when you wish to invoke your tasks, the data is there.  You have a CD platform?  Register me.  I can run on our GitHub, GitLab, Bamboo, CircleCI, ArgoCD, or any other CD system to take the artefacts from your builds and upload them to the Core Automation artefact repository and kick off the infrastructure deployment that your application needs.  I also trap all the logs so your CD platform can record every step I take for the task you've requested.

### Layer 13 - Core UI
Every system needs a dashboard.  And I come through for you! I am the Core UI. I am a GUI who allows you to see everything that has been deployed in our AWS infrastructure.  With me, you can create AWS accounts, set them up to be managed. Register portfolios and applications, establish Landing Zones, define networks, tagging policy, and see every Core Automation Component that has been deployed in your application.  You need one dashboard to see that all deployments were successful? I've got you covered.

### Part 14 - Core Docs
I'm not exactly a layer of the Core Automation, but I deserve mentioning. I'm a complete User's Guide and reference guide for Core Automation.  I allow you to find details on how to create templates that can be compiled into CloudFormation, how to create actions, how to define your own actions! and how to even use your own CloudFormation Templates.  This is full documentation to how to manage AWS Landing Zones at scale.

### Part 15 - Core Docker Base
I am what you would call the "Golden Image" operating system to be used by the Core Docker image builder.  I setup the foundational stable services so you only need to build me once.

### Part 16 - Core Docker
You may not wish to deploy ANY of the Core Automation system to Lambda.  That's OK. You can use me.  I'm a fully functional Core Automation Platform in a docker container.  Point me to a persistent volume and deploy me on docker desktop or Kubernetes, I'll run just fine.  Keep me running, and you can even use my Core API and Core UI services.  You can keep several of me running, it's ok.  But, I do need that external DynamoDB on AWS.  Sorry, not putting local-dynamodb in just yet. I'm also the guy you'll want to spin up if you wish to begin installing Core Automation as lambda and AWS API Gateway services.  Not the only way, but easiest.

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
    kind: AWS::KMS::CreateGrants
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

## Getting Started

See the [Core Docs](../sck-core-docs/README.md) for installation and usage instructions.

## Documentation

Comprehensive user and reference guides are available in the [Core Docs](../sck-core-docs/README.md) repository.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the GPL-3.0 License. See [LICENSE](LICENSE)
