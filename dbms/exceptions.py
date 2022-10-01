from typing import Optional

from dbms.request import Request
from dbms.response import Response


class DbmsError(Exception):
    """Base class for all exceptions in python-dbms."""


class DbmsClientError(DbmsError):
    """Base class for errors originating from python-dbms client.

    :param msg: Error message.
    :type msg: str

    :cvar source: Source of the error (always set to "client").
    :vartype source: str
    :ivar message: Error message.
    :vartype message: str
    """

    source = "client"

    def __init__(self, msg: str) -> None:
        super().__init__(msg)
        self.message = msg
        self.error_message = None
        self.error_code = None
        self.url = None
        self.response = None
        self.request = None
        self.http_method = None
        self.http_code = None
        self.http_headers = None


class DbmsServerError(DbmsError):
    """Base class for errors originating from DbmsDB server.

    :param resp: HTTP response.
    :type resp: dbms.response.Response
    :param msg: Error message override.
    :type msg: str

    :cvar source: Source of the error (always set to "server").
    :vartype source: str
    :ivar message: Exception message.
    :vartype message: str
    :ivar url: API URL.
    :vartype url: str
    :ivar response: HTTP response object.
    :vartype response: dbms.response.Response
    :ivar request: HTTP request object.
    :vartype request: dbms.request.Request
    :ivar http_method: HTTP method in lowercase (e.g. "post").
    :vartype http_method: str
    :ivar http_code: HTTP status code.
    :vartype http_code: int
    :ivar http_headers: Response headers.
    :vartype http_headers: dict
    :ivar error_code: Error code from DbmsDB server.
    :vartype error_code: int
    :ivar error_message: Raw error message from DbmsDB server.
    :vartype error_message: str
    """

    source = "server"

    def __init__(
        self, resp: Response, request: Request, msg: Optional[str] = None
    ) -> None:
        msg = msg or resp.error_message or resp.status_text
        self.error_message = resp.error_message
        self.error_code = resp.error_code
        if self.error_code is not None:
            msg = f"[HTTP {resp.status_code}][ERR {self.error_code}] {msg}"
        else:
            msg = f"[HTTP {resp.status_code}] {msg}"
            self.error_code = resp.status_code
        super().__init__(msg)
        self.message = msg
        self.url = resp.url
        self.response = resp
        self.request = request
        self.http_method = resp.method
        self.http_code = resp.status_code
        self.http_headers = resp.headers


##################
# AQL Exceptions #
##################


class AQLQueryListError(DbmsServerError):
    """Failed to retrieve running AQL queries."""


class AQLQueryExplainError(DbmsServerError):
    """Failed to parse and explain query."""


class AQLQueryValidateError(DbmsServerError):
    """Failed to parse and validate query."""


class AQLQueryExecuteError(DbmsServerError):
    """Failed to execute query."""


class AQLQueryKillError(DbmsServerError):
    """Failed to kill the query."""


class AQLQueryClearError(DbmsServerError):
    """Failed to clear slow AQL queries."""


class AQLQueryTrackingGetError(DbmsServerError):
    """Failed to retrieve AQL tracking properties."""


class AQLQueryTrackingSetError(DbmsServerError):
    """Failed to configure AQL tracking properties."""


class AQLCachePropertiesError(DbmsServerError):
    """Failed to retrieve query cache properties."""


class AQLCacheConfigureError(DbmsServerError):
    """Failed to configure query cache properties."""


class AQLCacheEntriesError(DbmsServerError):
    """Failed to retrieve AQL cache entries."""


class AQLCacheClearError(DbmsServerError):
    """Failed to clear the query cache."""


class AQLFunctionListError(DbmsServerError):
    """Failed to retrieve AQL user functions."""


class AQLFunctionCreateError(DbmsServerError):
    """Failed to create AQL user function."""


class AQLFunctionDeleteError(DbmsServerError):
    """Failed to delete AQL user function."""


class AQLQueryRulesGetError(DbmsServerError):
    """Failed to retrieve AQL query rules."""


##############################
# Async Execution Exceptions #
##############################


class AsyncExecuteError(DbmsServerError):
    """Failed to execute async API request."""


class AsyncJobListError(DbmsServerError):
    """Failed to retrieve async jobs."""


class AsyncJobCancelError(DbmsServerError):
    """Failed to cancel async job."""


class AsyncJobStatusError(DbmsServerError):
    """Failed to retrieve async job status."""


class AsyncJobResultError(DbmsServerError):
    """Failed to retrieve async job result."""


class AsyncJobClearError(DbmsServerError):
    """Failed to clear async job results."""


##############################
# Backup Exceptions #
##############################


class BackupCreateError(DbmsServerError):
    """Failed to create a backup."""


class BackupDeleteError(DbmsServerError):
    """Failed to delete a backup."""


class BackupDownloadError(DbmsServerError):
    """Failed to download a backup from remote repository."""


class BackupGetError(DbmsServerError):
    """Failed to retrieve backup details."""


class BackupRestoreError(DbmsServerError):
    """Failed to restore from backup."""


class BackupUploadError(DbmsServerError):
    """Failed to upload a backup to remote repository."""


##############################
# Batch Execution Exceptions #
##############################


class BatchStateError(DbmsClientError):
    """The batch object was in a bad state."""


class BatchJobResultError(DbmsClientError):
    """Failed to retrieve batch job result."""


class BatchExecuteError(DbmsServerError):
    """Failed to execute batch API request."""


#########################
# Collection Exceptions #
#########################


class CollectionListError(DbmsServerError):
    """Failed to retrieve collections."""


class CollectionPropertiesError(DbmsServerError):
    """Failed to retrieve collection properties."""


class CollectionConfigureError(DbmsServerError):
    """Failed to configure collection properties."""


class CollectionStatisticsError(DbmsServerError):
    """Failed to retrieve collection statistics."""


class CollectionRevisionError(DbmsServerError):
    """Failed to retrieve collection revision."""


class CollectionChecksumError(DbmsServerError):
    """Failed to retrieve collection checksum."""


class CollectionCreateError(DbmsServerError):
    """Failed to create collection."""


class CollectionDeleteError(DbmsServerError):
    """Failed to delete collection."""


class CollectionRenameError(DbmsServerError):
    """Failed to rename collection."""


class CollectionTruncateError(DbmsServerError):
    """Failed to truncate collection."""


class CollectionLoadError(DbmsServerError):
    """Failed to load collection."""


class CollectionUnloadError(DbmsServerError):
    """Failed to unload collection."""


class CollectionRecalculateCountError(DbmsServerError):
    """Failed to recalculate document count."""


class CollectionResponsibleShardError(DbmsServerError):
    """Failed to retrieve responsible shard."""


#####################
# Cursor Exceptions #
#####################


class CursorStateError(DbmsClientError):
    """The cursor object was in a bad state."""


class CursorCountError(DbmsClientError, TypeError):
    """The cursor count was not enabled."""


class CursorEmptyError(DbmsClientError):
    """The current batch in cursor was empty."""


class CursorNextError(DbmsServerError):
    """Failed to retrieve the next result batch from server."""


class CursorCloseError(DbmsServerError):
    """Failed to delete the cursor result from server."""


#######################
# Database Exceptions #
#######################


class DatabaseListError(DbmsServerError):
    """Failed to retrieve databases."""


class DatabasePropertiesError(DbmsServerError):
    """Failed to retrieve database properties."""


class DatabaseCreateError(DbmsServerError):
    """Failed to create database."""


class DatabaseDeleteError(DbmsServerError):
    """Failed to delete database."""


#######################
# Document Exceptions #
#######################


class DocumentParseError(DbmsClientError):
    """Failed to parse document input."""


class DocumentCountError(DbmsServerError):
    """Failed to retrieve document count."""


class DocumentInError(DbmsServerError):
    """Failed to check whether document exists."""


class DocumentGetError(DbmsServerError):
    """Failed to retrieve document."""


class DocumentKeysError(DbmsServerError):
    """Failed to retrieve document keys."""


class DocumentIDsError(DbmsServerError):
    """Failed to retrieve document IDs."""


class DocumentInsertError(DbmsServerError):
    """Failed to insert document."""


class DocumentReplaceError(DbmsServerError):
    """Failed to replace document."""


class DocumentUpdateError(DbmsServerError):
    """Failed to update document."""


class DocumentDeleteError(DbmsServerError):
    """Failed to delete document."""


class DocumentRevisionError(DbmsServerError):
    """The expected and actual document revisions mismatched."""


###################
# Foxx Exceptions #
###################


class FoxxServiceListError(DbmsServerError):
    """Failed to retrieve Foxx services."""


class FoxxServiceGetError(DbmsServerError):
    """Failed to retrieve Foxx service metadata."""


class FoxxServiceCreateError(DbmsServerError):
    """Failed to create Foxx service."""


class FoxxServiceUpdateError(DbmsServerError):
    """Failed to update Foxx service."""


class FoxxServiceReplaceError(DbmsServerError):
    """Failed to replace Foxx service."""


class FoxxServiceDeleteError(DbmsServerError):
    """Failed to delete Foxx services."""


class FoxxConfigGetError(DbmsServerError):
    """Failed to retrieve Foxx service configuration."""


class FoxxConfigUpdateError(DbmsServerError):
    """Failed to update Foxx service configuration."""


class FoxxConfigReplaceError(DbmsServerError):
    """Failed to replace Foxx service configuration."""


class FoxxDependencyGetError(DbmsServerError):
    """Failed to retrieve Foxx service dependencies."""


class FoxxDependencyUpdateError(DbmsServerError):
    """Failed to update Foxx service dependencies."""


class FoxxDependencyReplaceError(DbmsServerError):
    """Failed to replace Foxx service dependencies."""


class FoxxScriptListError(DbmsServerError):
    """Failed to retrieve Foxx service scripts."""


class FoxxScriptRunError(DbmsServerError):
    """Failed to run Foxx service script."""


class FoxxTestRunError(DbmsServerError):
    """Failed to run Foxx service tests."""


class FoxxDevModeEnableError(DbmsServerError):
    """Failed to enable development mode for Foxx service."""


class FoxxDevModeDisableError(DbmsServerError):
    """Failed to disable development mode for Foxx service."""


class FoxxReadmeGetError(DbmsServerError):
    """Failed to retrieve Foxx service readme."""


class FoxxSwaggerGetError(DbmsServerError):
    """Failed to retrieve Foxx service swagger."""


class FoxxDownloadError(DbmsServerError):
    """Failed to download Foxx service bundle."""


class FoxxCommitError(DbmsServerError):
    """Failed to commit local Foxx service state."""


####################
# Graph Exceptions #
####################


class GraphListError(DbmsServerError):
    """Failed to retrieve graphs."""


class GraphCreateError(DbmsServerError):
    """Failed to create the graph."""


class GraphDeleteError(DbmsServerError):
    """Failed to delete the graph."""


class GraphPropertiesError(DbmsServerError):
    """Failed to retrieve graph properties."""


class GraphTraverseError(DbmsServerError):
    """Failed to execute graph traversal."""


class VertexCollectionListError(DbmsServerError):
    """Failed to retrieve vertex collections."""


class VertexCollectionCreateError(DbmsServerError):
    """Failed to create vertex collection."""


class VertexCollectionDeleteError(DbmsServerError):
    """Failed to delete vertex collection."""


class EdgeDefinitionListError(DbmsServerError):
    """Failed to retrieve edge definitions."""


class EdgeDefinitionCreateError(DbmsServerError):
    """Failed to create edge definition."""


class EdgeDefinitionReplaceError(DbmsServerError):
    """Failed to replace edge definition."""


class EdgeDefinitionDeleteError(DbmsServerError):
    """Failed to delete edge definition."""


class EdgeListError(DbmsServerError):
    """Failed to retrieve edges coming in and out of a vertex."""


####################
# Index Exceptions #
####################


class IndexListError(DbmsServerError):
    """Failed to retrieve collection indexes."""


class IndexCreateError(DbmsServerError):
    """Failed to create collection index."""


class IndexDeleteError(DbmsServerError):
    """Failed to delete collection index."""


class IndexLoadError(DbmsServerError):
    """Failed to load indexes into memory."""


#####################
# Pregel Exceptions #
#####################


class PregelJobCreateError(DbmsServerError):
    """Failed to create Pregel job."""


class PregelJobGetError(DbmsServerError):
    """Failed to retrieve Pregel job details."""


class PregelJobDeleteError(DbmsServerError):
    """Failed to delete Pregel job."""


#####################
# Server Exceptions #
#####################


class ServerConnectionError(DbmsClientError):
    """Failed to connect to DbmsDB server."""


class ServerEngineError(DbmsServerError):
    """Failed to retrieve database engine."""


class ServerVersionError(DbmsServerError):
    """Failed to retrieve server version."""


class ServerDetailsError(DbmsServerError):
    """Failed to retrieve server details."""


class ServerStatusError(DbmsServerError):
    """Failed to retrieve server status."""


class ServerTimeError(DbmsServerError):
    """Failed to retrieve server system time."""


class ServerEchoError(DbmsServerError):
    """Failed to retrieve details on last request."""


class ServerShutdownError(DbmsServerError):
    """Failed to initiate shutdown sequence."""


class ServerRunTestsError(DbmsServerError):
    """Failed to execute server tests."""


class ServerRequiredDBVersionError(DbmsServerError):
    """Failed to retrieve server target version."""


class ServerReadLogError(DbmsServerError):
    """Failed to retrieve global log."""


class ServerLogLevelError(DbmsServerError):
    """Failed to retrieve server log levels."""


class ServerLogLevelSetError(DbmsServerError):
    """Failed to set server log levels."""


class ServerReloadRoutingError(DbmsServerError):
    """Failed to reload routing details."""


class ServerStatisticsError(DbmsServerError):
    """Failed to retrieve server statistics."""


class ServerMetricsError(DbmsServerError):
    """Failed to retrieve server metrics."""


class ServerRoleError(DbmsServerError):
    """Failed to retrieve server role in a cluster."""


class ServerTLSError(DbmsServerError):
    """Failed to retrieve TLS data."""


class ServerTLSReloadError(DbmsServerError):
    """Failed to reload TLS."""


class ServerEncryptionError(DbmsServerError):
    """Failed to reload user-defined encryption keys."""


#####################
# Task Exceptions   #
#####################


class TaskListError(DbmsServerError):
    """Failed to retrieve server tasks."""


class TaskGetError(DbmsServerError):
    """Failed to retrieve server task details."""


class TaskCreateError(DbmsServerError):
    """Failed to create server task."""


class TaskDeleteError(DbmsServerError):
    """Failed to delete server task."""


##########################
# Transaction Exceptions #
##########################


class TransactionExecuteError(DbmsServerError):
    """Failed to execute raw transaction."""


class TransactionInitError(DbmsServerError):
    """Failed to initialize transaction."""


class TransactionStatusError(DbmsServerError):
    """Failed to retrieve transaction status."""


class TransactionCommitError(DbmsServerError):
    """Failed to commit transaction."""


class TransactionAbortError(DbmsServerError):
    """Failed to abort transaction."""


###################
# User Exceptions #
###################


class UserListError(DbmsServerError):
    """Failed to retrieve users."""


class UserGetError(DbmsServerError):
    """Failed to retrieve user details."""


class UserCreateError(DbmsServerError):
    """Failed to create user."""


class UserUpdateError(DbmsServerError):
    """Failed to update user."""


class UserReplaceError(DbmsServerError):
    """Failed to replace user."""


class UserDeleteError(DbmsServerError):
    """Failed to delete user."""


###################
# View Exceptions #
###################


class ViewListError(DbmsServerError):
    """Failed to retrieve views."""


class ViewGetError(DbmsServerError):
    """Failed to retrieve view details."""


class ViewCreateError(DbmsServerError):
    """Failed to create view."""


class ViewUpdateError(DbmsServerError):
    """Failed to update view."""


class ViewReplaceError(DbmsServerError):
    """Failed to replace view."""


class ViewDeleteError(DbmsServerError):
    """Failed to delete view."""


class ViewRenameError(DbmsServerError):
    """Failed to rename view."""


#######################
# Analyzer Exceptions #
#######################


class AnalyzerListError(DbmsServerError):
    """Failed to retrieve analyzers."""


class AnalyzerGetError(DbmsServerError):
    """Failed to retrieve analyzer details."""


class AnalyzerCreateError(DbmsServerError):
    """Failed to create analyzer."""


class AnalyzerDeleteError(DbmsServerError):
    """Failed to delete analyzer."""


#########################
# Permission Exceptions #
#########################


class PermissionListError(DbmsServerError):
    """Failed to list user permissions."""


class PermissionGetError(DbmsServerError):
    """Failed to retrieve user permission."""


class PermissionUpdateError(DbmsServerError):
    """Failed to update user permission."""


class PermissionResetError(DbmsServerError):
    """Failed to reset user permission."""


##################
# WAL Exceptions #
##################


class WALPropertiesError(DbmsServerError):
    """Failed to retrieve WAL properties."""


class WALConfigureError(DbmsServerError):
    """Failed to configure WAL properties."""


class WALTransactionListError(DbmsServerError):
    """Failed to retrieve running WAL transactions."""


class WALFlushError(DbmsServerError):
    """Failed to flush WAL."""


class WALTickRangesError(DbmsServerError):
    """Failed to return WAL tick ranges."""


class WALLastTickError(DbmsServerError):
    """Failed to return WAL tick ranges."""


class WALTailError(DbmsServerError):
    """Failed to return WAL tick ranges."""


##########################
# Replication Exceptions #
##########################


class ReplicationInventoryError(DbmsServerError):
    """Failed to retrieve inventory of collection and indexes."""


class ReplicationDumpBatchCreateError(DbmsServerError):
    """Failed to create dump batch."""


class ReplicationDumpBatchDeleteError(DbmsServerError):
    """Failed to delete a dump batch."""


class ReplicationDumpBatchExtendError(DbmsServerError):
    """Failed to extend a dump batch."""


class ReplicationDumpError(DbmsServerError):
    """Failed to retrieve collection content."""


class ReplicationSyncError(DbmsServerError):
    """Failed to synchronize data from remote."""


class ReplicationClusterInventoryError(DbmsServerError):
    """Failed to retrieve overview of collection and indexes in a cluster."""


class ReplicationLoggerStateError(DbmsServerError):
    """Failed to retrieve logger state."""


class ReplicationLoggerFirstTickError(DbmsServerError):
    """Failed to retrieve logger first tick."""


class ReplicationApplierConfigError(DbmsServerError):
    """Failed to retrieve replication applier configuration."""


class ReplicationApplierConfigSetError(DbmsServerError):
    """Failed to update replication applier configuration."""


class ReplicationApplierStartError(DbmsServerError):
    """Failed to start replication applier."""


class ReplicationApplierStopError(DbmsServerError):
    """Failed to stop replication applier."""


class ReplicationApplierStateError(DbmsServerError):
    """Failed to retrieve replication applier state."""


class ReplicationMakeSlaveError(DbmsServerError):
    """Failed to change role to slave."""


class ReplicationServerIDError(DbmsServerError):
    """Failed to retrieve server ID."""


######################
# Cluster Exceptions #
######################


class ClusterHealthError(DbmsServerError):
    """Failed to retrieve DBServer health."""


class ClusterServerIDError(DbmsServerError):
    """Failed to retrieve server ID."""


class ClusterServerRoleError(DbmsServerError):
    """Failed to retrieve server role."""


class ClusterServerStatisticsError(DbmsServerError):
    """Failed to retrieve DBServer statistics."""


class ClusterServerVersionError(DbmsServerError):
    """Failed to retrieve server node version."""


class ClusterServerEngineError(DbmsServerError):
    """Failed to retrieve server node engine."""


class ClusterMaintenanceModeError(DbmsServerError):
    """Failed to enable/disable cluster supervision maintenance mode."""


class ClusterEndpointsError(DbmsServerError):
    """Failed to retrieve cluster endpoints."""


class ClusterServerCountError(DbmsServerError):
    """Failed to retrieve cluster server count."""


##################
# JWT Exceptions #
##################


class JWTAuthError(DbmsServerError):
    """Failed to get a new JWT token from DbmsDB."""


class JWTSecretListError(DbmsServerError):
    """Failed to retrieve information on currently loaded JWT secrets."""


class JWTSecretReloadError(DbmsServerError):
    """Failed to reload JWT secrets."""
