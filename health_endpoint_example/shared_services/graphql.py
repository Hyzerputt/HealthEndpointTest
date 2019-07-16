#! usr/bin/env python3.7
"""
Library to support the creation and exectuion of graphql queries and mutations
"""
import json
import datetime
from typing import Dict, Optional, Type

import requests
from requests.exceptions import ConnectionError
from pydantic import BaseModel
from loguru import logger

from .utils.retry import retry_call


__all__ = ["GraphQLClient"]


class DatetimeEncoder(json.JSONEncoder):
    """
    A JSON encoder which encodes datetimes as iso formatted strings.
    """

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat("T")
        return super().default(obj)


RETRY_INDEFINITELY = -1
RETRY_OFF = 1


class GraphQLClient:
    """
    Helper class for formatting and executing GraphQL queries and mutations
    """

    def __init__(self, gql_uri: str = "http://127.0.0.1:5000/graphql", retry_on_connection_error: bool = True):
        """

        :param gql_uri: fully qualified URI for the graphQL endpoint
        :param retry_on_connection_error: Boolena defaults to True.  If True queries/mutations
        will be retried indefinitely if a connection error occurs.
        """
        self.gql_host = gql_uri
        if retry_on_connection_error:
            self.retries = RETRY_INDEFINITELY  # Retry indefinitely
        else:
            self.retries = RETRY_OFF  # Don't retry

    @staticmethod
    def _graphql_response_from_model(model_cls: Type[BaseModel]) -> str:
        """
        Generate a GraphQL Response query from the class for the response.
        :param model_cls: Model representing the desired response object from the graphql endpoint
        :return: Portion of a graphql query which specifies the response object
        """

        def parse_field(field):
            if hasattr(field.type_, "__fields__"):
                return field.name, [parse_field(sfield) for sfield in field.type_.__fields__.values()]
            else:
                return field.name

        names = [parse_field(f) for f in model_cls.__fields__.values()]
        names = [f"{name[0]} {{ {' '.join(name[1])} }}" if isinstance(name, tuple) else name for name in names]
        return ", ".join(names)

    @staticmethod
    def _graphql_query_parameters_from_model(model: BaseModel) -> str:
        """
        Generate a GraphQL query parameters from the class for the query.
        :param model: Model representing the query parameters and actual search values (min 1)python
        :return: Portion of a graphql query which specifies the query parameters
        """

        # Only create query parameters for those with values using graphql syntax mask for the query parameters
        parameters = ", ".join([f"{k}: {json.dumps(v, cls=DatetimeEncoder)}" for k, v in model if v])

        return parameters

    def get_query(
        self, query_base: str, query_response_cls: Type[BaseModel], query_parameters: Optional[BaseModel] = None
    ) -> Dict[str, str]:
        """
        Create a GraphQL formatted query string
        :param query_base: Name of the root type to be queried
        :param query_parameters: Optional. Instance of a pydantic model containing attributes corresponding
        to parameter names and values corresponding to the parameter value.
        :param query_response_cls: A pydantic model class representing the structure of the response object
        with attributes  corresponding to the Graphql type and attribute names
        :return: Dictionary that can be passed as json to the GraphQL API endpoint
        """
        # Construct graphql query
        gql_query = query_base
        if query_parameters:
            gql_query += f"({self._graphql_query_parameters_from_model(query_parameters)})"
        gql_query += f"{{{self._graphql_response_from_model(query_response_cls)}}}"
        return {"query": f"{{{gql_query} }}"}

    def get_mutation(
        self, mutation_base: str, mutation_parameters: BaseModel, mutation_response_cls: Optional[BaseModel] = None
    ) -> Dict[str, str]:
        """
        Create a GraphQL formatted mutation string
        :param mutation_base: Name of the root type to be mutated
        :param mutation_parameters: Instance of a pydantic model containing attributes corresponding to
        parameter names and values corresponding to the parameter value.
        :param mutation_response_cls: Optional. A pydantic model class representing the structure of the
        response object with attributes corresponding to the Graphql type and attribute names.
        :return: Dictionary that can be passed as json to the GraphQL API endpoint
        """
        # Construct graphql mutation
        gql_mutation = (
            f"mutation {mutation_base} {{{mutation_base}"
            f"({self._graphql_query_parameters_from_model(mutation_parameters)}){{ok"
        )
        if mutation_response_cls:
            gql_mutation += f", {self._graphql_response_from_model(mutation_response_cls)}"
        gql_mutation += "}"

        return {"query": f"{gql_mutation} }}", "operationName": mutation_base}

    def execute_gql_call(self, query: dict) -> dict:
        """
        Executes a GraphQL query or mutation
        :param query: Dictionary formatted graphql query
        :return: Dictionary containing the response from the GraphQL endpoint
        """
        logger.debug(f"Execution graphql call: host={self.gql_host}, query={query}")
        response = retry_call(
            requests.post,
            fkwargs={"url": self.gql_host, "json": query},
            exceptions=ConnectionError,
            tries=self.retries,
            delay=1,
            backoff=2,
            max_delay=30,
            logger=logger,
        )
        # GraphQL encodes most errors within the body of a 200 response.  Non 200 statuses represent something outside
        # of the query parsing and execution having failed.
        if response.status_code > 299:
            raise ValueError(
                f"Server returned invalid response: " f"code=HTTP{response.status_code}, " f"detail={response.json()} "
            )
        return response.json()

    def execute_gql_query(
        self, query_base: str, query_response_cls: Type[BaseModel], query_parameters: Optional[BaseModel]
    ) -> dict:
        """
        Executes a graphql query based upon input pydantic models
        :param query_base: Name of the root type to be queried
        :param query_parameters: Optional. Instance of a pydantic model containing attributes corresponding to
        parameter names and values corresponding to the parameter value.
        :param query_response_cls: A pydantic model class representing the structure of the response
        object with attributes corresponding to the Graphql type and attribute names
        :return: Dictionary containing the response from the GraphQL endpoint
        """
        query = self.get_query(query_base, query_response_cls, query_parameters)
        return self.execute_gql_call(query)

    def execute_gql_mutation(
        self, mutation_base: str, mutation_parameters: BaseModel, mutation_response_cls: Optional[BaseModel] = None
    ) -> dict:
        """
        Executes a graphql mutation based upon input pydantic models
        :param mutation_base: Name of the root type to be mutated
        :param mutation_parameters: Instance of a pydantic model containing attributes corresponding to
        parameter names and values corresponding to the parameter value.
        :param mutation_response_cls: Optional. A pydantic model class representing the structure of the
        response object with attributes corresponding to the Graphql type and attribute names.
        :return: Dictionary containing the response from the GraphQL endpoint
        """
        mutation = self.get_mutation(mutation_base, mutation_parameters, mutation_response_cls)
        return self.execute_gql_call(mutation)
