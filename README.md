In Dart/Flutter there's no good way to serialize a Hash Table/Map and persisit it. One needs to EITHER create own serilizer/formatter OR use 3rd party solution (such as SQL Lite or Hive DB) which can be an overkill for simple scenarious.

IkvPack solves the problem and can save a given Map<String, String> to a file (on dartvm/dart:io, i.e. on native platforms such as Android or Windows) or Indexed DB (Web/dart:html) and lazily load it into memory. No native dependencies, pure Dart code.

This repo underpins the lookup functionality of a [dictionary app](https://github.com/maxim-saplin/dikt) and  has a number of features that make it more than just a Map serializer/deserializer:
1. All values are automatically compressed via Deflate and stored in binary shape, when requesting a value for a given key value is decompressed on the fly
2. When loading into memory (from file or IndexedDB) only keys are loaded
3. Values are loaded and decompressed lazily, i.e. when one fetches it by key
4. Keys are indexed which makes it possible to conduct super fast binary searches (rather than iterate through all keys in all typical Dictionary where there's no key access by index)
5. There's a fast binary search implementation of keyStartingWith() which retunrs a list of matching keys
6. Lookups by key support case insensitive mode and are also fast (by up-front building the list of lower case 'shadow' keys) 
7. No arbitrary types are allowed for Keys and Values, only strings
8. The collection is optimized for fast key access, and slow value fetching. This assumes scenarious with frequent key and infrequent value access (since the values are not stored in memory and are compressed and require on0the-go decompression)
9. There's support for isolates, you can parallelize loading multiple IkvFiles in separate isolates for faster performance
10. IkvPack can be loaded and kept in isolate (without the need to marashl instance to main thread and potentially block it) and used via proxy

Initially I used HiveDB to store and load key/value pairs and it is fast compared to other Flutter/Dart DB options (though I had to tweak it to allow non ASCII keys). At first it was OK, looking up among keys by iterating all of them (there're no indexed to keys and it is not possible to do binary searchs) and lazily loading boxes worked fine with dictionaries sized at hundreds of thousands of entreis and ~50MB of data. Though when stress testing with millions of entries and couple of gigabytes of data Hive became the bottleneck. Below is a speed comparison of several scenarios with 45MB WordNet 3 dictionary with 148730 entries (tested on dart vm, only ASCII keys, non ASCII will give greater advantage to IkvPack):

| Use case                          | IkvPack           | Hive              |
| --------------------------------- |:-----------------:| -----------------:|
| Loading IkvPack file/Hive box     | 25.6 ms           | 732.0 ms          |
| Iterating through all keys        | 7.3 microseconds  | 40.0 microseconds |
| Searcing for keys starting with %x| 0.5 microseconds  | 62.2 microseconds |

