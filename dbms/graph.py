__all__ = ["Graph"]

from typing import List, Optional, Sequence, Union

from dbms.api import ApiGroup
from dbms.relation import EdgeRelation, VertexRelation
from dbms.connection import Connection
from dbms.exceptions import (
    EdgeDefinitionCreateError,
    EdgeDefinitionDeleteError,
    EdgeDefinitionListError,
    EdgeDefinitionReplaceError,
    GraphPropertiesError,
    GraphTraverseError,
    VertexRelationCreateError,
    VertexRelationDeleteError,
    VertexRelationListError,
)
from dbms.executor import ApiExecutor
from dbms.formatter import format_graph_properties
from dbms.request import Request
from dbms.response import Response
from dbms.result import Result
from dbms.typings import Json, Jsons
from dbms.utils import get_col_name, get_doc_id


class Graph(ApiGroup):
    """Graph API wrapper."""

    def __init__(
        self, connection: Connection, executor: ApiExecutor, name: str
    ) -> None:
        super().__init__(connection, executor)
        self._name = name

    def __repr__(self) -> str:
        return f"<Graph {self._name}>"

    def _get_col_by_vertex(self, vertex: Union[str, Json]) -> VertexRelation:
        """Return the vertex relation for the given vertex document.

        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | dict
        :return: Vertex relation API wrapper.
        :rtype: dbms.relation.VertexRelation
        """
        return self.vertex_relation(get_col_name(vertex))

    def _get_col_by_edge(self, edge: Union[str, Json]) -> EdgeRelation:
        """Return the edge relation for the given edge document.

        :param edge: Edge document ID or body with "_id" field.
        :type edge: str | dict
        :return: Edge relation API wrapper.
        :rtype: dbms.relation.EdgeRelation
        """
        return self.edge_relation(get_col_name(edge))

    @property
    def name(self) -> str:
        """Return the graph name.

        :return: Graph name.
        :rtype: str
        """
        return self._name

    def properties(self) -> Result[Json]:
        """Return graph properties.

        :return: Graph properties.
        :rtype: dict
        :raise dbms.exceptions.GraphPropertiesError: If retrieval fails.
        """
        request = Request(method="get", endpoint=f"/_api/gharial/{self._name}")

        def response_handler(resp: Response) -> Json:
            if resp.is_success:
                return format_graph_properties(resp.body["graph"])
            raise GraphPropertiesError(resp, request)

        return self._execute(request, response_handler)

    ################################
    # Vertex Relation Management #
    ################################

    def has_vertex_relation(self, name: str) -> Result[bool]:
        """Check if the graph has the given vertex relation.

        :param name: Vertex relation name.
        :type name: str
        :return: True if vertex relation exists, False otherwise.
        :rtype: bool
        """
        request = Request(
            method="get",
            endpoint=f"/_api/gharial/{self._name}/vertex",
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return name in resp.body["collections"]
            raise VertexRelationListError(resp, request)

        return self._execute(request, response_handler)

    def vertex_relations(self) -> Result[List[str]]:
        """Return vertex relations in the graph that are not orphaned.

        :return: Names of vertex relations that are not orphaned.
        :rtype: [str]
        :raise dbms.exceptions.VertexRelationListError: If retrieval fails.
        """
        request = Request(
            method="get",
            endpoint=f"/_api/gharial/{self._name}/vertex",
        )

        def response_handler(resp: Response) -> List[str]:
            if not resp.is_success:
                raise VertexRelationListError(resp, request)
            return sorted(set(resp.body["collections"]))

        return self._execute(request, response_handler)

    def vertex_relation(self, name: str) -> VertexRelation:
        """Return the vertex relation API wrapper.

        :param name: Vertex relation name.
        :type name: str
        :return: Vertex relation API wrapper.
        :rtype: dbms.relation.VertexRelation
        """
        return VertexRelation(self._conn, self._executor, self._name, name)

    def create_vertex_relation(self, name: str) -> Result[VertexRelation]:
        """Create a vertex relation in the graph.

        :param name: Vertex relation name.
        :type name: str
        :return: Vertex relation API wrapper.
        :rtype: dbms.relation.VertexRelation
        :raise dbms.exceptions.VertexRelationCreateError: If create fails.
        """
        request = Request(
            method="post",
            endpoint=f"/_api/gharial/{self._name}/vertex",
            data={"relation": name},
        )

        def response_handler(resp: Response) -> VertexRelation:
            if resp.is_success:
                return self.vertex_relation(name)
            raise VertexRelationCreateError(resp, request)

        return self._execute(request, response_handler)

    def delete_vertex_relation(self, name: str, purge: bool = False) -> Result[bool]:
        """Remove a vertex relation from the graph.

        :param name: Vertex relation name.
        :type name: str
        :param purge: If set to True, the vertex relation is not just deleted
            from the graph but also from the database completely.
        :type purge: bool
        :return: True if vertex relation was deleted successfully.
        :rtype: bool
        :raise dbms.exceptions.VertexRelationDeleteError: If delete fails.
        """
        request = Request(
            method="delete",
            endpoint=f"/_api/gharial/{self._name}/vertex/{name}",
            params={"dropCollection": purge},
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            raise VertexRelationDeleteError(resp, request)

        return self._execute(request, response_handler)

    ##############################
    # Edge Relation Management #
    ##############################

    def has_edge_definition(self, name: str) -> Result[bool]:
        """Check if the graph has the given edge definition.

        :param name: Edge relation name.
        :type name: str
        :return: True if edge definition exists, False otherwise.
        :rtype: bool
        """
        request = Request(method="get", endpoint=f"/_api/gharial/{self._name}")

        def response_handler(resp: Response) -> bool:
            if not resp.is_success:
                raise EdgeDefinitionListError(resp, request)

            body = resp.body["graph"]
            return any(
                edge_definition["relation"] == name
                for edge_definition in body["edgeDefinitions"]
            )

        return self._execute(request, response_handler)

    def has_edge_relation(self, name: str) -> Result[bool]:
        """Check if the graph has the given edge relation.

        :param name: Edge relation name.
        :type name: str
        :return: True if edge relation exists, False otherwise.
        :rtype: bool
        """
        return self.has_edge_definition(name)

    def edge_relation(self, name: str) -> EdgeRelation:
        """Return the edge relation API wrapper.

        :param name: Edge relation name.
        :type name: str
        :return: Edge relation API wrapper.
        :rtype: dbms.relation.EdgeRelation
        """
        return EdgeRelation(self._conn, self._executor, self._name, name)

    def edge_definitions(self) -> Result[Jsons]:
        """Return the edge definitions of the graph.

        :return: Edge definitions of the graph.
        :rtype: [dict]
        :raise dbms.exceptions.EdgeDefinitionListError: If retrieval fails.
        """
        request = Request(method="get", endpoint=f"/_api/gharial/{self._name}")

        def response_handler(resp: Response) -> Jsons:
            if not resp.is_success:
                raise EdgeDefinitionListError(resp, request)

            body = resp.body["graph"]
            return [
                {
                    "edge_relation": edge_definition["relation"],
                    "from_vertex_relations": edge_definition["from"],
                    "to_vertex_relations": edge_definition["to"],
                }
                for edge_definition in body["edgeDefinitions"]
            ]

        return self._execute(request, response_handler)

    def create_edge_definition(
        self,
        edge_relation: str,
        from_vertex_relations: Sequence[str],
        to_vertex_relations: Sequence[str],
    ) -> Result[EdgeRelation]:
        """Create a new edge definition.

        An edge definition consists of an edge relation, "from" vertex
        relation(s) and "to" vertex relation(s). Here is an example entry:

        .. code-block:: python

            {
                'edge_relation': 'edge_relation_name',
                'from_vertex_relations': ['from_vertex_relation_name'],
                'to_vertex_relations': ['to_vertex_relation_name']
            }

        :param edge_relation: Edge relation name.
        :type edge_relation: str
        :param from_vertex_relations: Names of "from" vertex relations.
        :type from_vertex_relations: [str]
        :param to_vertex_relations: Names of "to" vertex relations.
        :type to_vertex_relations: [str]
        :return: Edge relation API wrapper.
        :rtype: dbms.relation.EdgeRelation
        :raise dbms.exceptions.EdgeDefinitionCreateError: If create fails.
        """
        request = Request(
            method="post",
            endpoint=f"/_api/gharial/{self._name}/edge",
            data={
                "relation": edge_relation,
                "from": from_vertex_relations,
                "to": to_vertex_relations,
            },
        )

        def response_handler(resp: Response) -> EdgeRelation:
            if resp.is_success:
                return self.edge_relation(edge_relation)
            raise EdgeDefinitionCreateError(resp, request)

        return self._execute(request, response_handler)

    def replace_edge_definition(
        self,
        edge_relation: str,
        from_vertex_relations: Sequence[str],
        to_vertex_relations: Sequence[str],
    ) -> Result[EdgeRelation]:
        """Replace an edge definition.

        :param edge_relation: Edge relation name.
        :type edge_relation: str
        :param from_vertex_relations: Names of "from" vertex relations.
        :type from_vertex_relations: [str]
        :param to_vertex_relations: Names of "to" vertex relations.
        :type to_vertex_relations: [str]
        :return: Edge relation API wrapper.
        :rtype: dbms.relation.EdgeRelation
        :raise dbms.exceptions.EdgeDefinitionReplaceError: If replace fails.
        """
        request = Request(
            method="put",
            endpoint=f"/_api/gharial/{self._name}/edge/{edge_relation}",
            data={
                "relation": edge_relation,
                "from": from_vertex_relations,
                "to": to_vertex_relations,
            },
        )

        def response_handler(resp: Response) -> EdgeRelation:
            if resp.is_success:
                return self.edge_relation(edge_relation)
            raise EdgeDefinitionReplaceError(resp, request)

        return self._execute(request, response_handler)

    def delete_edge_definition(self, name: str, purge: bool = False) -> Result[bool]:
        """Delete an edge definition from the graph.

        :param name: Edge relation name.
        :type name: str
        :param purge: If set to True, the edge definition is not just removed
            from the graph but the edge relation is also deleted completely
            from the database.
        :type purge: bool
        :return: True if edge definition was deleted successfully.
        :rtype: bool
        :raise dbms.exceptions.EdgeDefinitionDeleteError: If delete fails.
        """
        request = Request(
            method="delete",
            endpoint=f"/_api/gharial/{self._name}/edge/{name}",
            params={"dropCollections": purge},
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            raise EdgeDefinitionDeleteError(resp, request)

        return self._execute(request, response_handler)

    ###################
    # Graph Functions #
    ###################

    def traverse(
        self,
        start_vertex: Union[str, Json],
        direction: str = "outbound",
        item_order: str = "forward",
        strategy: Optional[str] = None,
        order: Optional[str] = None,
        edge_uniqueness: Optional[str] = None,
        vertex_uniqueness: Optional[str] = None,
        max_iter: Optional[int] = None,
        min_depth: Optional[int] = None,
        max_depth: Optional[int] = None,
        init_func: Optional[str] = None,
        sort_func: Optional[str] = None,
        filter_func: Optional[str] = None,
        visitor_func: Optional[str] = None,
        expander_func: Optional[str] = None,
    ) -> Result[Json]:
        """Traverse the graph and return the visited vertices and edges.

        :param start_vertex: Start vertex document ID or body with "_id" field.
        :type start_vertex: str | dict
        :param direction: Traversal direction. Allowed values are "outbound"
            (default), "inbound" and "any".
        :type direction: str
        :param item_order: Item iteration order. Allowed values are "forward"
            (default) and "backward".
        :type item_order: str
        :param strategy: Traversal strategy. Allowed values are "depthfirst"
            and "breadthfirst".
        :type strategy: str | None
        :param order: Traversal order. Allowed values are "preorder",
            "postorder", and "preorder-expander".
        :type order: str | None
        :param edge_uniqueness: Uniqueness for visited edges. Allowed values
            are "global", "path" or "none".
        :type edge_uniqueness: str | None
        :param vertex_uniqueness: Uniqueness for visited vertices. Allowed
            values are "global", "path" or "none".
        :type vertex_uniqueness: str | None
        :param max_iter: If set, halt the traversal after the given number of
            iterations. This parameter can be used to prevent endless loops in
            cyclic graphs.
        :type max_iter: int | None
        :param min_depth: Minimum depth of the nodes to visit.
        :type min_depth: int | None
        :param max_depth: Maximum depth of the nodes to visit.
        :type max_depth: int | None
        :param init_func: Initialization function in Javascript with signature
            ``(config, result) -> void``. This function is used to initialize
            values in the result.
        :type init_func: str | None
        :param sort_func: Sorting function in Javascript with signature
            ``(left, right) -> integer``, which returns ``-1`` if ``left <
            right``, ``+1`` if ``left > right`` and ``0`` if ``left == right``.
        :type sort_func: str | None
        :param filter_func: Filter function in Javascript with signature
            ``(config, vertex, path) -> mixed``, where ``mixed`` can have one
            of the following values (or an array with multiple): "exclude" (do
            not visit the vertex), "prune" (do not follow the edges of the
            vertex), or "undefined" (visit the vertex and follow its edges).
        :type filter_func: str | None
        :param visitor_func: Visitor function in Javascript with signature
            ``(config, result, vertex, path, connected) -> void``. The return
            value is ignored, ``result`` is modified by reference, and
            ``connected`` is populated only when parameter **order** is set to
            "preorder-expander".
        :type visitor_func: str | None
        :param expander_func: Expander function in Javascript with signature
            ``(config, vertex, path) -> mixed``. The function must return an
            array of connections for ``vertex``. Each connection is an object
            with attributes "edge" and "vertex".
        :type expander_func: str | None
        :return: Visited edges and vertices.
        :rtype: dict
        :raise dbms.exceptions.GraphTraverseError: If traversal fails.
        """
        if strategy is not None:
            if strategy.lower() == "dfs":
                strategy = "depthfirst"
            elif strategy.lower() == "bfs":
                strategy = "breadthfirst"

        uniqueness = {}
        if vertex_uniqueness is not None:
            uniqueness["vertices"] = vertex_uniqueness
        if edge_uniqueness is not None:
            uniqueness["edges"] = edge_uniqueness

        data: Json = {
            "startVertex": get_doc_id(start_vertex),
            "graphName": self._name,
            "direction": direction,
            "strategy": strategy,
            "order": order,
            "itemOrder": item_order,
            "uniqueness": uniqueness or None,
            "maxIterations": max_iter,
            "minDepth": min_depth,
            "maxDepth": max_depth,
            "init": init_func,
            "filter": filter_func,
            "visitor": visitor_func,
            "sort": sort_func,
            "expander": expander_func,
        }
        request = Request(
            method="post",
            endpoint="/_api/traversal",
            data={k: v for k, v in data.items() if v is not None},
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise GraphTraverseError(resp, request)

            result: Json = resp.body["result"]["visited"]
            return result

        return self._execute(request, response_handler)

    #####################
    # Vertex Management #
    #####################

    def has_vertex(
        self,
        vertex: Union[str, Json],
        rev: Optional[str] = None,
        check_rev: bool = True,
    ) -> Result[bool]:
        """Check if the given vertex document exists in the graph.

        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **vertex** if present.
        :type rev: str | None
        :param check_rev: If set to True, revision of **vertex** (if given) is
            compared against the revision of target vertex document.
        :type check_rev: bool
        :return: True if vertex document exists, False otherwise.
        :rtype: bool
        :raise dbms.exceptions.DocumentGetError: If check fails.
        :raise dbms.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).has(vertex, rev, check_rev)

    def vertex(
        self,
        vertex: Union[str, Json],
        rev: Optional[str] = None,
        check_rev: bool = True,
    ) -> Result[Optional[Json]]:
        """Return a vertex document.

        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **vertex** if present.
        :type rev: str | None
        :param check_rev: If set to True, revision of **vertex** (if given) is
            compared against the revision of target vertex document.
        :type check_rev: bool
        :return: Vertex document or None if not found.
        :rtype: dict | None
        :raise dbms.exceptions.DocumentGetError: If retrieval fails.
        :raise dbms.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).get(vertex, rev, check_rev)

    def insert_vertex(
        self,
        relation: str,
        vertex: Json,
        sync: Optional[bool] = None,
        silent: bool = False,
    ) -> Result[Union[bool, Json]]:
        """Insert a new vertex document.

        :param relation: Vertex relation name.
        :type relation: str
        :param vertex: New vertex document to insert. If it has "_key" or "_id"
            field, its value is used as key of the new vertex (otherwise it is
            auto-generated). Any "_rev" field is ignored.
        :type vertex: dict
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool | None
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise dbms.exceptions.DocumentInsertError: If insert fails.
        """
        return self.vertex_relation(relation).insert(vertex, sync, silent)

    def update_vertex(
        self,
        vertex: Json,
        check_rev: bool = True,
        keep_none: bool = True,
        sync: Optional[bool] = None,
        silent: bool = False,
    ) -> Result[Union[bool, Json]]:
        """Update a vertex document.

        :param vertex: Partial or full vertex document with updated values. It
            must contain the "_id" field.
        :type vertex: dict
        :param check_rev: If set to True, revision of **vertex** (if given) is
            compared against the revision of target vertex document.
        :type check_rev: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. If set to False, they are removed completely.
        :type keep_none: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool | None
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise dbms.exceptions.DocumentUpdateError: If update fails.
        :raise dbms.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).update(
            vertex=vertex,
            check_rev=check_rev,
            keep_none=keep_none,
            sync=sync,
            silent=silent,
        )

    def replace_vertex(
        self,
        vertex: Json,
        check_rev: bool = True,
        sync: Optional[bool] = None,
        silent: bool = False,
    ) -> Result[Union[bool, Json]]:
        """Replace a vertex document.

        :param vertex: New vertex document to replace the old one with. It must
            contain the "_id" field.
        :type vertex: dict
        :param check_rev: If set to True, revision of **vertex** (if given) is
            compared against the revision of target vertex document.
        :type check_rev: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool | None
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise dbms.exceptions.DocumentReplaceError: If replace fails.
        :raise dbms.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).replace(
            vertex=vertex, check_rev=check_rev, sync=sync, silent=silent
        )

    def delete_vertex(
        self,
        vertex: Json,
        rev: Optional[str] = None,
        check_rev: bool = True,
        ignore_missing: bool = False,
        sync: Optional[bool] = None,
    ) -> Result[Union[bool, Json]]:
        """Delete a vertex document.

        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **vertex** if present.
        :type rev: str | None
        :param check_rev: If set to True, revision of **vertex** (if given) is
            compared against the revision of target vertex document.
        :type check_rev: bool
        :param ignore_missing: Do not raise an exception on missing document.
            This parameter has no effect in transactions where an exception is
            always raised on failures.
        :type ignore_missing: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool | None
        :return: True if vertex was deleted successfully, False if vertex was
            not found and **ignore_missing** was set to True (does not apply in
            transactions).
        :rtype: bool
        :raise dbms.exceptions.DocumentDeleteError: If delete fails.
        :raise dbms.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).delete(
            vertex=vertex,
            rev=rev,
            check_rev=check_rev,
            ignore_missing=ignore_missing,
            sync=sync,
        )

    ###################
    # Edge Management #
    ###################

    def has_edge(
        self, edge: Union[str, Json], rev: Optional[str] = None, check_rev: bool = True
    ) -> Result[bool]:
        """Check if the given edge document exists in the graph.

        :param edge: Edge document ID or body with "_id" field.
        :type edge: str | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **edge** if present.
        :type rev: str | None
        :param check_rev: If set to True, revision of **edge** (if given) is
            compared against the revision of target edge document.
        :type check_rev: bool
        :return: True if edge document exists, False otherwise.
        :rtype: bool
        :raise dbms.exceptions.DocumentInError: If check fails.
        :raise dbms.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).has(edge, rev, check_rev)

    def edge(
        self, edge: Union[str, Json], rev: Optional[str] = None, check_rev: bool = True
    ) -> Result[Optional[Json]]:
        """Return an edge document.

        :param edge: Edge document ID or body with "_id" field.
        :type edge: str | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **edge** if present.
        :type rev: str | None
        :param check_rev: If set to True, revision of **edge** (if given) is
            compared against the revision of target edge document.
        :type check_rev: bool
        :return: Edge document or None if not found.
        :rtype: dict | None
        :raise dbms.exceptions.DocumentGetError: If retrieval fails.
        :raise dbms.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).get(edge, rev, check_rev)

    def insert_edge(
        self,
        relation: str,
        edge: Json,
        sync: Optional[bool] = None,
        silent: bool = False,
    ) -> Result[Union[bool, Json]]:
        """Insert a new edge document.

        :param relation: Edge relation name.
        :type relation: str
        :param edge: New edge document to insert. It must contain "_from" and
            "_to" fields. If it has "_key" or "_id" field, its value is used
            as key of the new edge document (otherwise it is auto-generated).
            Any "_rev" field is ignored.
        :type edge: dict
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool | None
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise dbms.exceptions.DocumentInsertError: If insert fails.
        """
        return self.edge_relation(relation).insert(edge, sync, silent)

    def update_edge(
        self,
        edge: Json,
        check_rev: bool = True,
        keep_none: bool = True,
        sync: Optional[bool] = None,
        silent: bool = False,
    ) -> Result[Union[bool, Json]]:
        """Update an edge document.

        :param edge: Partial or full edge document with updated values. It must
            contain the "_id" field.
        :type edge: dict
        :param check_rev: If set to True, revision of **edge** (if given) is
            compared against the revision of target edge document.
        :type check_rev: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. If set to False, they are removed completely.
        :type keep_none: bool | None
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool | None
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise dbms.exceptions.DocumentUpdateError: If update fails.
        :raise dbms.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).update(
            edge=edge,
            check_rev=check_rev,
            keep_none=keep_none,
            sync=sync,
            silent=silent,
        )

    def replace_edge(
        self,
        edge: Json,
        check_rev: bool = True,
        sync: Optional[bool] = None,
        silent: bool = False,
    ) -> Result[Union[bool, Json]]:
        """Replace an edge document.

        :param edge: New edge document to replace the old one with. It must
            contain the "_id" field. It must also contain the "_from" and "_to"
            fields.
        :type edge: dict
        :param check_rev: If set to True, revision of **edge** (if given) is
            compared against the revision of target edge document.
        :type check_rev: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool | None
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise dbms.exceptions.DocumentReplaceError: If replace fails.
        :raise dbms.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).replace(
            edge=edge, check_rev=check_rev, sync=sync, silent=silent
        )

    def delete_edge(
        self,
        edge: Union[str, Json],
        rev: Optional[str] = None,
        check_rev: bool = True,
        ignore_missing: bool = False,
        sync: Optional[bool] = None,
    ) -> Result[Union[bool, Json]]:
        """Delete an edge document.

        :param edge: Edge document ID or body with "_id" field.
        :type edge: str | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **edge** if present.
        :type rev: str | None
        :param check_rev: If set to True, revision of **edge** (if given) is
            compared against the revision of target edge document.
        :type check_rev: bool
        :param ignore_missing: Do not raise an exception on missing document.
            This parameter has no effect in transactions where an exception is
            always raised on failures.
        :type ignore_missing: bool
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool | None
        :return: True if edge was deleted successfully, False if edge was not
            found and **ignore_missing** was set to True (does not  apply in
            transactions).
        :rtype: bool
        :raise dbms.exceptions.DocumentDeleteError: If delete fails.
        :raise dbms.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).delete(
            edge=edge,
            rev=rev,
            check_rev=check_rev,
            ignore_missing=ignore_missing,
            sync=sync,
        )

    def link(
        self,
        relation: str,
        from_vertex: Union[str, Json],
        to_vertex: Union[str, Json],
        data: Optional[Json] = None,
        sync: Optional[bool] = None,
        silent: bool = False,
    ) -> Result[Union[bool, Json]]:
        """Insert a new edge document linking the given vertices.

        :param relation: Edge relation name.
        :type relation: str
        :param from_vertex: "From" vertex document ID or body with "_id" field.
        :type from_vertex: str | dict
        :param to_vertex: "To" vertex document ID or body with "_id" field.
        :type to_vertex: str | dict
        :param data: Any extra data for the new edge document. If it has "_key"
            or "_id" field, its value is used as key of the new edge document
            (otherwise it is auto-generated).
        :type data: dict
        :param sync: Block until operation is synchronized to disk.
        :type sync: bool | None
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise dbms.exceptions.DocumentInsertError: If insert fails.
        """
        return self.edge_relation(relation).link(
            from_vertex=from_vertex,
            to_vertex=to_vertex,
            data=data,
            sync=sync,
            silent=silent,
        )

    def edges(
        self, relation: str, vertex: Union[str, Json], direction: Optional[str] = None
    ) -> Result[Json]:
        """Return the edge documents coming in and/or out of given vertex.

        :param relation: Edge relation name.
        :type relation: str
        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | dict
        :param direction: The direction of the edges. Allowed values are "in"
            and "out". If not set, edges in both directions are returned.
        :type direction: str
        :return: List of edges and statistics.
        :rtype: dict
        :raise dbms.exceptions.EdgeListError: If retrieval fails.
        """
        return self.edge_relation(relation).edges(vertex, direction)
