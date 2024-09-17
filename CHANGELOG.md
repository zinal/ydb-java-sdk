## 2.3.0 ##

* Core: fixed JWT token parser
* Query: updated TableClientimplementation

## 2.2.11 ##

* Table: Add unique index support
* Table: Updated table TTL implementation

## 2.2.10 ##

* Core: fixed infinite wainting for grpc channel ready state after reconnect

## 2.2.9 ##

* Query: added TableClient implementation
* Topics: fixed race condition on AsyncReaderImpl.shutdown()

## 2.2.8 ##

* Core: discovery errors log level changed to WARN instead of ERROR
* Topics: added handling of the unexpected exceptions in the client callbacks

## 2.2.7 ##

* Query: fixed NPE in QueryStream implementation
* OAuth2 provider: added support of mode with config file

## 2.2.6 ##

* Table: added support of NullType & PgType
* Core: improved grpc calls logging
* Query: added support of warning issues

## 2.2.5 ##

* Core: fixed work with ready operations

## 2.2.4 ##

* Core: fixed operations canceling and forgeting

## 2.2.3 ##

* Added modele with OAuth2 token exchange auth provider
* Core: added allocation-free helper for Result extraction
* Exports: add methods to find existing operations

## 2.2.2 ##

* Core: added option to configure transport initialization mode
* Tests: added option to use isolate docker environment

## 2.2.1 ##

* Topics: fix unordered reading in case of user errors
* Core: fixed non optional dependency of grpc-netty

## 2.2.0 ##

* Updated implementation of CoordinationService
* Added module for QueryService
* Added module for ExportService
* Added common module with basic interfaces for transactions and retries
* Core: Added basic support of long operations
* Core: Added support for tracing requests with id
* Table: Added readRows operation in table service
* Topics: Added support of transaction between tables and topics

## 2.1.13 ##

* Coordination: fixed timeout for coordination session

## 2.1.12 ##

* Test common: Changed visibility of YdbDockerContainer
* Core: Added async helpers to build the future chains

## 2.1.11 ##

* Transports: Add option withGrpcKeepAliveTime to enable grpc keep-alives
* Table: Fixed creation and altering of dataColumns indexes in createTable/alterTable
* Upgraded version of yc-auth to avoid jackson-databind vulnerability

## 2.1.10 ##

* Topics: Added message metadata support
* Topics: Added support for reading without a consumer
* Topics: Fixed a bug where onReaderClosed was not called on async reader shutdown
* Topics: Small logging improvements
* Added batch limit support for readTable

## 2.1.9 ##

* Topics: Fixed a bug where first commit was not getting commitResponse if a user had sent a custom StartPartitionSessionResponse
* Topics: Enhanced writer logging

## 2.1.8 ##

* Topics: Added DeferredCommitter class to group several read commits into one or just defer each commit without holding data reference
* Topics: Added onCommitResponse callback for AsyncReader to subscribe to server event directly and control commits more efficiently
* Topics: Removed usage of ForkJoinPool.commonPool()
* Table: Fixed typo in SessionPoolStats

## 2.1.7 ##

* Topics: Rethrow IO exceptions to user handlers while decoding messages
* Topics: Tie partition sessions to streams

## 2.1.6 ##

* Topics: Fixed a bug where topic writer / reader init() future was not completed under certain conditions
* Topics: Memory usage is now managed in topic reader within each session separately

## 2.1.5 ##

* GrpcTransport: Added async initialization
* GrpcTransport: Increased default timeout of the initialization waiting
* Topics: Added partitionId to StopPartitionSessionEvent
* Topics: Fixed a bug where Writer could end up receiving errors from server in a loop on restarts.
* Topics: Fixed a bug where not all errors were retried by Readers and Writers
* Topics: SyncReader refactored which also fixed possible memory leaks in it
* Topics: Fixed a bug where Writer.init future was not completed on shutdown
* Topics: Fixed a bug where Readers and Writers were not shutdown after closing transport they were using
* Topics: AsyncReader is now shutdown on any unhandled exception in user callback

## 2.1.4 ##

* Fixed partition session graceful shutdown
* Increased default timeouts for GrcpTransport
* Fixed NPE on empty discovery result

## 2.1.3 ##

* Enhanced message commit logging

## 2.1.2 ##

* Added withDiscoveryTimeout to GrpcTransportBuilder
* Added query statistics to DataQueryResult
* Fixed topic reader hang-up on server restart

## 2.1.1 ##

* Added commitOffset method to topic service
* Updated class Changefeed - added fields inititialScan, virtualTimestamps and retentionPeriod
* Added method executeReadTable with support of virtual timestamps
* Fixed session leaks if to cancel future from session pool
* Added addtional session pool metrics

## 2.1.0 ##

* Added config for GRPC compression
* Added Coordination Service API support
* Added Topic API support
* Added support detect local dc
* Support creating asynchronous index in createTable and describeTable
* New api for streamable requests
* Fixed default timeout for readTable
* Fixed exponential backoff calculation
* Fixed null values for Result.success
* Fixed bug of channels closing
* Changed default load balancer policy from pick_first to round_robin
* Added config for channel connect timeout
* Fixed discard of UnexpectedResultException causes
* All periodic tasks have been moved to unite scheduled executor service
* Added support of integration tests for JUnit4 and JUnit5
* Added bom module to simplify the import of SDK and its dependencies
* SchemeClient has been moved to its own module
* API of authentication providers has been separated into a detached module with its own versioning
* Removed unnecessary parameter EndpointsDiscoveryPeriod from GrpcTransportBuilder
* Fixed serialization/deserialization of inner optional types
* Added copyTables() consistent multi-table copy operation to the Session

## 2.0.1 ##

* Fixed broken sessions handling in the session pool

## 2.0.0 ##

* Added SnapshotRO trasaction level
* Added static credentials support
* New lock-free session pool
* New grpc transport with client-side balancing
* Clean api of YDB  types and values
* Removed inconcistent methods of Session and TableClient

## 1.14.5 ##

* Fixed casting for Decimal with scale

## 1.14.4 ##

* Added support of not null columns in TableDescription

## 1.14.3 ##

* Added request's costInfo to all operations

## 1.14.2 ##

* Remove use common FJ

## 1.14.1 ##

* Added support of server-side session balancer
* Added support of session graceful shutdown

## 1.14.0 ##

* Bind session to server node it was created at

## 1.13.8 ##

* Added support of changefeeds to alterTable

## 1.13.7 ##

* Added regular re-discovery for new YDB transport implementation
* Added node pessimization on receiving transport error in new YDB transport implementation

## 1.13.6 ##

* Added reportCostInfo flag to RequestSettings
* Added costInfo to DataQueryResult

## 1.13.5 ##

* Use cached version of IAM Auth Provider

## 1.13.4 ##

* Fix some issues in session pool

## 1.13.3 ##

* Added QueryStatsCollection.Mode parameter to ExecuteDataQuerySettings

## 1.13.2 ##

* Use TableDescription.getPartitioningSettings() by default, mark CreateTableSettings.getPartitioningSettings() deprected

## 1.13.1 ##

* Added data columns (for covered indexes) to TableDescription

## 1.13.0 ##

* Upgrade to use java-grpc 1.43.2

## 1.12.2 ##

* PreferNearestLoadBalancer and RandomChoiceLoadBalancer merged to YdbLoadBalancer
* Fixed problems with java-grpc > 1.26.x

## 1.12.1 ##

* Fixed PreferNearestLoadBalancer

## 1.12.0 ##

* Added YDB transport implementation (YDB_TRANSPORT_IMPL). Grpc transport implementation (GRPC_TRANSPORT_IMPL) is still used by default.
New transport implementation doesnt have noticable advantages over grpc implementation yet. But it makes room for some further improvements.

## 1.11.6 ##

* Removed internal proto files

## 1.11.5 ##

* Added module protobuf-api with proto files

## 1.11.4 ##

* Fix grpc-netty-shaded dependency

## 1.11.3 ##

* Removed double byte copy for getString(Charset charset)
* Added termination waiting for GrpcTransport

## 1.11.2 ##

* Fixed Netty TLS on JDK8 builds

## 1.11.1 ##

* Additional timing logs for Session

## 1.11.0 ##

* Added minPartitionsCount and maxPartitionsCount to PartitioningSettings
* Added PartitioningSettings and PartitionStats to describeTable result

## 1.10.11 ##

* Fix unwrapped CompletionException for getOrCreateSession

## 1.10.10 ##

* Fix retries counter in SessionRetryContext

## 1.10.9 ##

* Extended logging for Session and SessionRetryContext

## 1.10.8 ##

* Add URL Shortener demo application

## 1.10.7 ##

* Reuse HashedWheelTimer in GrpcOperationTray

## 1.10.6 ##

* Add JUnit tests with testcontainer support
* Add bulk upsert example

## 1.10.5 ##

* Lowered logging level for some messages.

## 1.10.4 ##

* Discovery host resolve check was softened. Now at least 1/2 resolved hosts of all that discovery returned is considered as a success resolve. This prevents from failing requests when some hosts can not be resolved.

## 1.10.3 ##

* Added logging for locks in session pool.

## 1.10.2 ##

* Added idempotent flag for SessionRetryContext which mark an operation as retryable.

## 1.10.1 ##

* Added public method tryToConnect() for GrpcTransport allowing to add any logic in case of connection timeout or other errors.

## 1.10.0 ##

* Added DiscoveryMode for GrpcTransport. SYNC mode is used by default, which means GRPCTransport will lock on constructor until grpc channel will be connected. This should improve session balancing on start with high load. It should also reduce latency of first requests.

## 1.9.3 ##

* Fix String comparsion in CloudAuthHelper.

## 1.9.2 ##

* Endpoint re-discovery now launches periodically as intended.

## 1.9.1 ##

* All timers are now stopped on shutdown so the application should stop immediately.

## 1.9.0 ##

* Added Random choice load balancer which is now used by default instead of round robin load balancer.

## 1.8.4 ##

* Use a bit more modern GRPC name resolver factory API to facilitate GRPC implementation upgrades downstream.

## 1.8.3 ##

* Added connection string support.

## 1.8.2 ##

* Added CloudAuthProvider for cloud authentication with environment variables.

## 1.8.1 ##

* Added AlterLogTable to ydb_logstore_v1.proto.

## 1.8.0 ##

* Added fast backoff settings to SessionRetryContext. ABORTED and UNAVAILABLE status codes are retried with fast backoff now.

## 1.7.10 ##

* Update junit from 4.12 to 4.13.1.
* Update protobuf from 3.6.1 to 4.15.6.

## 1.7.9 ##

* Disabling client query cache does not disable server-side query cache anymore.

## 1.7.8 ##

* Start initial changelog.
