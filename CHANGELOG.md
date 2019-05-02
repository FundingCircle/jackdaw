# Changelog

## [Unrelaesed]

### Added

 * Add changelog and contributing files (#122)
 * Add test-machine example to the word-count example application (#120)

### Changed

 * Ensure user-supplied partitions are cast to `int` before handing
   them to the underlying kafka producer (#124)
 * Make the test for the rest-proxy transport use keywords to identify topics (#120)
 * Resolve dependency conflict reported by lein deps :tree (#125)
 * Fix a typo in the `service-ready?` test fixture (#121)
 * Make sure mock dirver is closed after use (#128)
 * Fix Jackdaw version for Word Count example (#131)
 * Add new arrities for aggregate and reduce (#132)

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
