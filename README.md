# filebeats-message-extractor
Parse a filebeats kafka event. Split the message and the metadata in the event. Route the metadata into a KTable, and the message to another topic.

### Work in progress

```
CREATE OR REPLACE TABLE filebeats_metadata
(id 							VARCHAR PRIMARY KEY,
"agent.version.keyword"			Array<VARCHAR>,
"host.architecture.keyword"		Array<VARCHAR>,
"host.name.keyword"				Array<VARCHAR>,
"host.os.build.keyword"			Array<VARCHAR>,
"host.hostname"					Array<VARCHAR>,
"host.mac"						Array<VARCHAR>,
"agent.hostname.keyword"		Array<VARCHAR>,
"ecs.version.keyword"			Array<VARCHAR>,
"host.ip.keyword"				Array<VARCHAR>,
"host.os.version"				Array<VARCHAR>,
"host.os.name"					Array<VARCHAR>,
"agent.name"					Array<VARCHAR>,
"host.id.keyword"				Array<VARCHAR>,
"host.name"						Array<VARCHAR>,
"host.os.version.keyword"		Array<VARCHAR>,
"agent.id.keyword"				Array<VARCHAR>,
"input.type"					Array<VARCHAR>,
"@version.keyword"				Array<VARCHAR>,
"log.offset"					Array<VARCHAR>,
"log.flags"						Array<VARCHAR>,
"agent.hostname"				Array<VARCHAR>,
"host.architecture"				Array<VARCHAR>,
"agent.id"						Array<VARCHAR>,
"ecs.version"					Array<VARCHAR>,
"host.hostname.keyword"			Array<VARCHAR>,
"agent.version"					Array<VARCHAR>,
"host.os.family"				Array<VARCHAR>,
"input.type.keyword"			Array<VARCHAR>,
"host.os.build"					Array<VARCHAR>,
"host.ip"						Array<VARCHAR>,
"agent.type"					Array<VARCHAR>,
"host.os.kernel.keyword"		Array<VARCHAR>,
"log.flags.keyword"				Array<VARCHAR>,
"host.os.kernel"				Array<VARCHAR>,
"@version"						Array<VARCHAR>,
"host.os.name.keyword"			Array<VARCHAR>,
"host.id"						Array<VARCHAR>,
"log.file.path.keyword"			Array<VARCHAR>,
"agent.type.keyword"			Array<VARCHAR>,
"agent.ephemeral_id.keyword"	Array<VARCHAR>,
"host.mac.keyword"				Array<VARCHAR>,
"agent.name.keyword"			Array<VARCHAR>,
"host.os.family.keyword"		Array<VARCHAR>,
"host.os.platform"				Array<VARCHAR>,
"host.os.platform.keyword"		Array<VARCHAR>,
"log.file.path"					Array<VARCHAR>,
"agent.ephemeral_id"			Array<VARCHAR>)
WITH (KAFKA_TOPIC = 'filebeats-message-extractor-metadata-changelog',
      VALUE_FORMAT='JSON');
```

