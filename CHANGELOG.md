# Changelog

## Unreleased
* Drop clj-time dependency
* Added confluent schema registry support for JSON Schema
* Removed dependency on deprecated `org.apache.kafka.streams.kstream.Serialized` class

### Added

* Add KStream-KTable inner join.

## [0.8.0] - [2021-05-13]
* Update Kafka to 2.8.0 (confluent 6.1.1) [#292](https://github.com/FundingCircle/jackdaw/pull/292)
* Improve test-machine documentation [#287](https://github.com/FundingCircle/jackdaw/pull/287)
* Fix CI pipeline: add -repo to repo cache names to not match with deps cache [#288](https://github.com/FundingCircle/jackdaw/pull/288)
* Remove codecov [#289](https://github.com/FundingCircle/jackdaw/pull/289)

## [0.7.10] - [2021-04-14]
* Bump netty related packages to latest version (related to changes in `0.7.8` which fixes CVEs).

## [0.7.8] - [2021-03-01]
* Override the netty version pulled by Aleph with one which fixes https://nvd.nist.gov/vuln/detail/CVE-2020-11612 [#261](https://github.com/FundingCircle/jackdaw/pull/261)
* Restore the test fixture namespace [#266](https://github.com/FundingCircle/jackdaw/pull/266)

## [0.7.7] - [2021-02-09]

### Added

* Added serializer properties when creating an avro serializer

* Exposed deserializer and serializer properties in the serde-resolver.

## [0.7.6] - [2021-07-16]

### Added

* Added support for kafka message headers to Test Machine

## [0.7.5] - [2020-07-02] 

### Fixed

* Replaced deprecated methods in avro.clj  with supported methods.

## [0.7.4] - [2020-04-23]

### Fixed

* Fix `ktable` constructor to use supplied `store-name` 

* Bump `clj-uuid` version to `0.1.9`

* Minor update to fix harmless but distracting reflection warnings

## [0.7.3] - [2020-04-08]

### Added

 * Allow avro deserializaton via the resolver without a local copy of the schema

 * Start formalizing test-machine commands with fspec'd functions

### Fixed

 * Moved dependency on kafka_2.11 into the dev profile

## [0.7.2] - [2020-02-07]

### Fixed

* Fixed bug in Avro deserialisation, when handling a union of enum types

## [0.7.1] - [2020-02-06]

### Added

 * as-edn/as-json functions to convert between representations of avro

### Fixed

 * Fixed bug in `map->ProducerRecord`
 * Allow nullable `partition` and `timestamp` in `->ProducerRecord` (previously threw NPE)
 * Fixed union type serialisation when members have similar fields

## [0.7.0] - [2019-12-19]

### Added

 * Fressian Serde via clojure.data.fressian #209
 * Clearer error from the command runner #214
 * Documentation about Jackdaw Admin API #211
 * Upgraded in #217:
   * Kafka client version to 2.3.1: https://kafka.apache.org/23/documentation.html#upgrade_230_notable
   * Confluent Platform components version to 5.3.1: https://docs.confluent.io/5.3.1/release-notes/index.html#cp-5-3-1-release-notes
 * Added functions to simplify querying the test machine journal (#215)

## [0.6.9] - [2019-10-16]

### Added

 * Upgrade the test-runner (#184)
 * Added support for user-provided parameters to reset-application-fixture (#177)
 * Continuing refinement of examples (#191)
 * Support for `.suppress` (#23)
 * Support for group-options when creating rest-proxy client for the test-machine (#206)
 * PR Template (#187)
 * A new edn serde without the un-necessary newline (#190)

### Fixed

 * Fail fast throwing an exception as soon as a command fails (#186)
 * Throw an exception on unknown commands, useful to detect typos early (#182)
 * Fixed add-key (part of publishing pipeline) (#181)
 * Skip deploy_snapshots job for external contributors (#194)
 * Only numbers should be coercable (#203)
 * Small refactor to implementation of rest-proxy transport (#205)
 * topics-ready? does not dereference the returned deferred (#193)

## [0.6.8] - [2019-08-22]

### Fixed

 * Fix test machine status middleware (#157)
 * Fix application reset fixture to use bootstrap servers form app-config (#157)
 * Fix type-hint call to `KafkaAvroDeserializer.deseialize` to remove warning (#157)

## [0.6.7] - [2019-07-30]

### Added

 * Allow specification of :deserialization-properties (#157)
 * Back-fill a few tests of jackdaw.client.partitioning (#165)

### Changed

 * Upgrade Clojure version to 1.10.1 (#159)
 * Partitioner in test-machine write command updated to match streams (#139)
 * Reformatted all the code using cljfmt (#173)

### Fixed

 * Delete duplicated tests (#165)
 * Documentation/Examples fixes (#166, #168)
 * Do not assume result of executing command is a map (#164)
 * Supply `key-serde` as well as `value-serde` in `aggregate` methods (#172)

## [0.6.6] - [2019-06-20]

### Added

 * Auto-coercion of clojure numbers if possible (#135)
 * Added more explicit information about commit signing Contributing guide (#150)

### Fixed

 * Merger instance required for session window aggregation (#142)
 * Fixed mis-leading parameter names relating to global ktables (#147)
 * Select matching record from union during serialization (#149)
 * Fixed typo in one of the code examples in the streams guide (#151)

## [0.6.5] - [2019-06-14]

### Added

 * Add Add `:do!` and `:inspect` test commands (#141)

### Changed

 * Fix regression in multi-topic stream constructor (#143)
 * Stop swallowing errors in test-machine (#144)
 * Include kafka-streams-test-utils so users don't have to (#138)

## [0.6.4] - [2019-05-02]

### Added

 * Add changelog and contributing files (#122)
 * Add test-machine example to the word-count example application (#120)
 * Add new arities for aggregate and reduce (#132)
 * Add sign-off to contributing file (#123)

### Changed

 * Ensure user-supplied partitions are cast to `int` before handing
   them to the underlying kafka producer (#124)
 * Make the test for the rest-proxy transport use keywords to identify topics (#120)
 * Resolve dependency conflict reported by lein deps :tree (#125)
 * Fix a typo in the `service-ready?` test fixture (#121)
 * Make sure mock dirver is closed after use (#128)
 * Fix Jackdaw version for Word Count example (#131)
 * Update changelog for 0.6.4 release (#124)

### Removed

 * Delete a couple of overly verbose logging statements (#124)


## [0.6.3] - [2019-03-28]

### Added

None

### Changed

 * Upgrade Kafka dependency to 2.2.0 (#123)

### Removed

None


## [0.6.2] - [2019-03-21]

### Added

 * Improvement and clarification of documentation (on-going) (#116, #117, #118)
 * Support for including a literal avro schema in the topic definitions resolved by the default resolver (#109)
 * Support for including a custom :partition-fn in kafka streams operations that write records (#103)

### Changed

 * Implement kibit recommendations (#118)
 * Word-count example (#108)
   - Log to file instead of stdout
   - A few simplifications in the implementation
 * In the streams mock driver, get-records now returns a vector of "datafied" producer-records rather than simply the k,v pairs (part of #103)

### Removed

None
