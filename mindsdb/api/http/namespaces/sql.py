import traceback

from flask import request
from flask_restx import Resource

import mindsdb.utilities.hooks as hooks
import mindsdb.utilities.profiler as profiler
from mindsdb.api.http.namespaces.configs.sql import ns_conf
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.data_types.response_type import (
    RESPONSE_TYPE as SQL_RESPONSE_TYPE,
)
from mindsdb.api.executor.exceptions import ExecutorException, UnknownError
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx

from mindsdb_sql import parse_sql
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import Constant, Identifier, Select, Join
from mindsdb_sql.planner.utils import query_traversal

logger = log.getLogger(__name__)


@ns_conf.route("/query")
@ns_conf.param("query", "Execute query")
class Query(Resource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    

    @ns_conf.doc("query")
    @api_endpoint_metrics('POST', '/sql/query')
    def post(self):
        query = request.json["query"]
        context = request.json.get("context", {})

        if context.get("profiling") is True:
            profiler.enable()

        error_type = None
        error_code = None
        error_text = None
        error_traceback = None

        profiler.set_meta(
            query=query, api="http", environment=Config().get("environment")
        )
        with profiler.Context("http_query_processing"):
            mysql_proxy = FakeMysqlProxy()
            mysql_proxy.set_context(context)
            try:
                result = mysql_proxy.process_query(query)

                if result.type == SQL_RESPONSE_TYPE.OK:
                    query_response = {"type": SQL_RESPONSE_TYPE.OK}
                elif result.type == SQL_RESPONSE_TYPE.TABLE:
                    query_response = {
                        "type": SQL_RESPONSE_TYPE.TABLE,
                        "data": result.data,
                        "column_names": [
                            x["alias"] or x["name"] if "alias" in x else x["name"]
                            for x in result.columns
                        ],
                    }
            except ExecutorException as e:
                # classified error
                error_type = "expected"
                query_response = {
                    "type": SQL_RESPONSE_TYPE.ERROR,
                    "error_code": 0,
                    "error_message": str(e),
                }

            except UnknownError as e:
                # unclassified
                error_type = "unexpected"
                query_response = {
                    "type": SQL_RESPONSE_TYPE.ERROR,
                    "error_code": 0,
                    "error_message": str(e),
                }

            except Exception as e:
                error_type = "unexpected"
                query_response = {
                    "type": SQL_RESPONSE_TYPE.ERROR,
                    "error_code": 0,
                    "error_message": str(e),
                }
                logger.error(f"Error profiling query: \n{traceback.format_exc()}")

            if query_response.get("type") == SQL_RESPONSE_TYPE.ERROR:
                error_type = "expected"
                error_code = query_response.get("error_code")
                error_text = query_response.get("error_message")

            context = mysql_proxy.get_context()

            query_response["context"] = context

        hooks.after_api_query(
            company_id=ctx.company_id,
            api="http",
            command=None,
            payload=query,
            error_type=error_type,
            error_code=error_code,
            error_text=error_text,
            traceback=error_traceback,
        )

        return query_response, 200

@ns_conf.route("/query/constants")
@ns_conf.param("query", "Get Constants for the query")
class QueryConstants(Resource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def find_constants_with_identifiers(self, node, replace_constants=False, identifiers_to_replace={}):
        identifier_to_constant = {}
        identifier_count = {}
        last_identifier = None


        def callback(n, **kwargs):
            nonlocal last_identifier
            
            if isinstance(n, Identifier):
                last_identifier = n
            elif isinstance(n, Constant):
                if last_identifier:
                    identifier_str = last_identifier.get_string()
                    if identifier_str in identifier_count:
                        identifier_count[identifier_str] += 1
                        identifier_str += str(identifier_count[identifier_str])
                    else:
                        identifier_count[identifier_str] = 0
                    
                    identifier_to_constant[identifier_str] = (last_identifier.get_string(), n.value, type(n.value).__name__)
                    
                    if replace_constants and identifier_str in identifiers_to_replace:
                        n.value = '@' + identifiers_to_replace[identifier_str]
                    last_identifier = None  # Reset after associating with a Constant
            return None

        query_traversal(node, callback)
        return identifier_to_constant

    def get_children(self, node):
      if hasattr(node, 'children'):
          return node.children
      elif isinstance(node, Select):
          children = []
          
          if node.from_table:
              children.append(node.from_table)
          if node.cte:
              # TODO: Handle CTEs
              pass  
          return children
      elif isinstance(node, ast.Join):
          children = []
          if node.left:
              children.append(node.left)
          if node.right:
              children.append(node.right)
          return children
      else:
          return []

    def find_datasource(self, node):
        datasource_node = None
        max_depth = -1

        def traverse(node, depth=0):
            nonlocal datasource_node, max_depth
            if isinstance(node, Identifier):
                if depth > max_depth:
                    datasource_node = node
                    max_depth = depth
            for child in self.get_children(node):
                traverse(child, depth + 1)

        traverse(node)
        if len(datasource_node.parts) <= 1:
            return ""
        
        return datasource_node.parts[0]
            
        

    @ns_conf.doc("query_constants")
    @api_endpoint_metrics('POST', '/sql/query/constants')
    def post(self):
        query = request.json["query"]
        replace_constants = request.json.get("replace_constants", False)
        identifiers_to_replace = request.json.get("identifiers_to_replace", {})
        context = request.json.get("context", {})

        try:
            query_ast = parse_sql(query)
            parameterized_query = query
            datasource = ""
            constants_with_identifiers = self.find_constants_with_identifiers(query_ast,replace_constants=replace_constants, identifiers_to_replace=identifiers_to_replace)
            if replace_constants:
                parameterized_query = query_ast.to_string()
            else:
                datasource = self.find_datasource(query_ast)
            response = {
                "constant_with_identifiers": constants_with_identifiers,
                "parameterized_query": parameterized_query,
                "datasource": datasource
            }
            query_response = {"type": SQL_RESPONSE_TYPE.OK, "data": response}
        except Exception as e:
            query_response = {
                "type": SQL_RESPONSE_TYPE.ERROR,
                "error_code": 0,
                "error_message": str(e),
            }

        return query_response, 200
    
    

@ns_conf.route("/list_databases")
@ns_conf.param("list_databases", "lists databases of mindsdb")
class ListDatabases(Resource):
    @ns_conf.doc("list_databases")
    @api_endpoint_metrics('GET', '/sql/list_databases')
    def get(self):
        listing_query = "SHOW DATABASES"
        mysql_proxy = FakeMysqlProxy()
        try:
            result = mysql_proxy.process_query(listing_query)

            # iterate over result.data and perform a query on each item to get the name of the tables
            if result.type == SQL_RESPONSE_TYPE.ERROR:
                listing_query_response = {
                    "type": "error",
                    "error_code": result.error_code,
                    "error_message": result.error_message,
                }
            elif result.type == SQL_RESPONSE_TYPE.OK:
                listing_query_response = {"type": "ok"}
            elif result.type == SQL_RESPONSE_TYPE.TABLE:
                listing_query_response = {
                    "data": [
                        {
                            "name": x[0],
                            "tables": mysql_proxy.process_query(
                                "SHOW TABLES FROM `{}`".format(x[0])
                            ).data,
                        }
                        for x in result.data
                    ]
                }
        except Exception as e:
            listing_query_response = {
                "type": "error",
                "error_code": 0,
                "error_message": str(e),
            }

        return listing_query_response, 200
