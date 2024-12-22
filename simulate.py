#!/usr/bin/python3
"""Poor man's sfn-local (i.e. https://github.com/awslabs/aws-sam-local/)."""
import argparse
import time
import json
import os
import re
import yaml

from core_execute import handler as lambda_handler


def _get_args():
    parser = argparse.ArgumentParser(
        description="Component Compiler for the Action Runner",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "action", choices=["deploy", "release", "teardown"], help="Action to perform"
    )
    parser.add_argument(
        "-c", "--client", help="Client name for selecting config", required=True
    )
    parser.add_argument("-p", "--portfolio", help="Portfolio name", required=True)
    parser.add_argument("-a", "--app", help="Application name", required=True)
    parser.add_argument("-b", "--branch", help="Branch name", required=True)
    parser.add_argument("-n", "--build", help="Build number", required=True)
    parser.add_argument("--client-vars", help="Client vars")
    parser.add_argument(
        "--aws-profile",
        default=None,
        help="Select which profile to use from your ~/.aws/credentials file.",
    )
    args = parser.parse_args()
    if args.action is None:
        raise ValueError("action is required to simulate.")
    return args


def run(args):
    action = args.action
    client = args.client
    portfolio = args.portfolio
    app = args.app
    branch = args.branch
    build = args.build

    branch_short_name = re.sub(r"[^a-z0-9\\-]", "-", branch.lower())[0:20].rstrip("-")

    # Load client vars and set into env.
    # FIXME Urgh.... args vs client-vars.yaml
    client_vars_file = "../../../{}-config/client-vars.yaml".format(client)
    if args.client_vars is not None:
        client_vars_file = args.client_vars

    with open(client_vars_file) as f:
        client_vars = yaml.safe_load(f.read())
    for key in client_vars:
        os.environ[key] = "{}".format(client_vars[key])
    print("client_vars={}".format(json.dumps(client_vars, indent=2)))

    # Set AWS_PROFILE, if supplied
    if args.aws_profile:
        os.environ["AWS_PROFILE"] = args.aws_profile

    bucket_region = client_vars["CLIENT_REGION"]
    bucket_name = "{}{}-core-automation-{}".format(
        client_vars.get("SCOPE_PREFIX", ""),
        client_vars["CLIENT_NAME"],
        client_vars["CLIENT_REGION"],
    )

    delivered_by = (
        os.environ["DELIVERED_BY"] if "DELIVERED_BY" in os.environ else "automation"
    )

    event = {
        "Actions": {
            "Key": "artefacts/{}/{}/{}/{}/{}.actions".format(
                portfolio, app, branch_short_name, build, action
            ),
            "BucketRegion": bucket_region,
            "BucketName": bucket_name,
        },
        "DeploymentDetails": {
            "DeliveredBy": delivered_by,
            "Portfolio": portfolio,
            "App": app,
            "Branch": branch,
            "Build": build,
            "BranchShortName": branch_short_name,
        },
        "Identity": "prn:{}:{}:{}:{}".format(portfolio, app, branch_short_name, build),
        "Task": action,
        "State": {
            "Key": "artefacts/{}/{}/{}/{}/{}.state".format(
                portfolio, app, branch_short_name, build, action
            ),
            "VersionId": "new",
            "BucketRegion": bucket_region,
            "BucketName": bucket_name,
        },
    }
    # exit('event={}'.format(json.dumps(event, indent=2)))

    while True:
        event = lambda_handler(event, {})

        # print("StepFunction: Transitioning to state '{}'".format(event["FlowControl"]))

        # print("FlowControl: {}".format(event["FlowControl"]))
        if event["FlowControl"] == "wait":
            time.sleep(15)
        elif event["FlowControl"] == "success":
            print("====== SUCCESS! ======")
            print(json.dumps(event, indent=2))
            f = open("simulate-response.txt", "w")
            f.write(json.dumps(event, indent=2))
            f.close()
            break
        elif event["FlowControl"] == "failure":
            print("====== FAILURE! ======")
            print(json.dumps(event, indent=2))
            f = open("simulate-response.txt", "w")
            f.write(json.dumps(event, indent=2))
            f.close()
            break
        elif event["FlowControl"] == "execute":
            pass
        else:
            print(
                "====== ERROR: Unknown flow control '{}' ======".format(
                    event["FlowControl"]
                )
            )
            break


if __name__ == "__main__":
    args = _get_args()
    run(args)
