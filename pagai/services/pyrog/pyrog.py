import os
import requests

from pagai.errors import OperationOutcome

PYROG_URL = os.getenv("PYROG_URL")
PYROG_LOGIN = os.getenv("PYROG_LOGIN")
PYROG_PASSWORD = os.getenv("PYROG_PASSWORD")

login_mutation = """
mutation login($email: String!, $password: String!) {
  login(email: $email, password: $password) {
    token
  }
}
"""

resource_query = """
query resource($resourceId: ID!) {
    resource(resourceId: $resourceId) {
        id
        filters {
            id
            sqlColumn {
                id
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
                owner
                login
                password: decryptedPassword
            }
        }
    }
}
"""


class PyrogClient:
    def __init__(self):
        self.token = self.login()

    def get_headers(self, auth_required=True):
        if auth_required and not self.token:
            raise OperationOutcome(
                "PyrogClient is not authenticated (login has probably failed, check your logs)"
            )
        headers = {"content-type": "application/json"}
        if auth_required:
            headers["Authorization"] = f"Bearer {self.token}"

        return headers

    def run_graphql_query(self, graphql_query, variables=None, auth_required=True):
        """
        This function queries a GraphQL endpoint
        and returns a json parsed response. If auth_required is true, the auth token
        will be passed in an Authorization header (an error will be raised if the token is missing).
        """
        if not PYROG_URL:
            raise OperationOutcome("PYROG_URL is missing from environment")

        try:
            response = requests.post(
                PYROG_URL,
                headers=self.get_headers(auth_required),
                json={"query": graphql_query, "variables": variables},
            )
        except requests.exceptions.ConnectionError:
            raise OperationOutcome("Could not connect to the Pyrog service")

        if response.status_code != 200:
            raise Exception(
                "Graphql query failed with returning code "
                f"{response.status_code}\n{response.json()}."
            )
        body = response.json()
        if "errors" in body:
            raise Exception(f"GraphQL query failed with errors: {body['errors']}.")

        return body

    def login(self):
        if not PYROG_LOGIN or not PYROG_PASSWORD:
            raise OperationOutcome(
                "PYROG_LOGIN and PYROG_PASSWORD env variables must be set in environment"
            )
        resp = self.run_graphql_query(
            login_mutation,
            variables={"email": PYROG_LOGIN, "password": PYROG_PASSWORD},
            auth_required=False,
        )
        data = resp["data"]
        if not data:
            raise OperationOutcome(
                f"Could not login to pyrog (email={PYROG_LOGIN}): {resp['errors'][0]['message']}"
            )
        return data["login"]["token"]

    def get_resource(self, resource_id):
        resp = self.run_graphql_query(resource_query, variables={"resourceId": resource_id})
        resource = resp["data"]["resource"]
        if not resource:
            raise OperationOutcome(f"Resource with id {resource_id} does not exist")
        return resource
