import os
import requests

from pagai.errors import OperationOutcome

PYROG_URL = os.getenv("PYROG_URL")
PYROG_TOKEN = os.getenv("PYROG_TOKEN")

resource_query = """
query resource($resourceId: ID!) {
    resource(resourceId: $resourceId) {
        id
        filters {
            id
            sqlColumn {
                id
                owner
                table
                column
            }
            relation
            value
        }
        source {
            id
            credential {
                model
                host
                port
                database
                login
                password
            }
        }
    }
}
"""


def get_headers():
    if not PYROG_TOKEN:
        raise OperationOutcome(
            "PYROG_TOKEN is missing from environment: cannot get database credentials"
        )
    return {"content-type": "application/json", "Authorization": f"Bearer {PYROG_TOKEN}"}


def run_graphql_query(graphql_query, variables=None):
    """
    This function queries a GraphQL endpoint
    and returns a json parsed response.
    """
    if not PYROG_URL:
        raise OperationOutcome(
            "PYROG_URL is missing from environment: cannot get database credentials"
        )

    try:
        response = requests.post(
            PYROG_URL, headers=get_headers(), json={"query": graphql_query, "variables": variables}
        )
    except requests.exceptions.ConnectionError:
        raise OperationOutcome("Could not connect to the Pyrog service")

    if response.status_code != 200:
        raise Exception(
            f"Graphql query failed with returning code {response.status_code}\n{response.json()}."
        )
    body = response.json()
    if "errors" in body:
        raise Exception(f"GraphQL query failed with errors: {body['errors']}.")

    return body

def get_resource(resource_id):
    resp = run_graphql_query(resource_query, variables={"resourceId": resource_id})
    resource = resp["data"]["resource"]
    if not resource:
        raise OperationOutcome(f"Resource with id {resource_id} does not exist")
    return resource
