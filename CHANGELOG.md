# Changelog

## [Unreleased]

### Added

### Changed

### Removed

## [0.6.2] - [2019-03-21]

Work continues on improving the documentation/examples and improving upstream API coverage.

### Added

 * improvement and clarification of documentation (on-going) (#116, #117, #118)
 * support for including a literal avro schema in the topic definitions resolved by the default resolver (#109)
 * support for including a custom :partition-fn in kafka streams operations that write records (#103)

### Changed

 * implement kibit recommendations (#118)
 * word-count example (#108)
   - log to file instead of stdout
   - a few simplifications in the implementation
 * in the streams mock driver, get-records now returns a vector of "datafied" producer-records rather than simply the k,v pairs (part of #103)

### Removed

None
