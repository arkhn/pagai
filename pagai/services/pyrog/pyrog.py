import os
import requests

from pagai.errors import AuthenticationError, AuthorizationError, OperationOutcome

PYROG_URL = os.getenv("PYROG_URL")

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
    def __init__(self, auth_header, id_token):
        if not auth_header:
            # Note that the id token is not mandatory because pyrog-server can introspect the
            # access to token with Hydra
            raise OperationOutcome(
                "An authorization token is required to forward queries to Pyrog-server"
            )
        self.headers = {
            "content-type": "application/json",
            "Authorization": auth_header,
            "IdToken": id_token,
        }

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
                headers=self.headers,
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
            status_code = body["errors"][0].get("statusCode")
            error_message = body["errors"][0].get("message")
            if status_code == 401:
                raise AuthenticationError(error_message)
            if status_code == 403:
                raise AuthorizationError("You don't have the rights to perform this action.")
            raise Exception(f"GraphQL query failed with errors: {body['errors']}.")

        return body

    def get_resource(self, resource_id):
        resp = self.run_graphql_query(resource_query, variables={"resourceId": resource_id})
        resource = resp["data"]["resource"]
        if not resource:
            raise OperationOutcome(f"Resource with id {resource_id} does not exist")
        return resource
