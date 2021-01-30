In Dart/Flutter there's no good way to serialize a Hash Table/Map and persisit it, one needs either to create own serilizer/formatter OR use 3rd party DB (such as SQL Lite or Hive) which can be an overkill for this simple scenario.

IkvPack solves the problem and can save a given Map<String, String> to a file (on dartvm/dart:io, i.e. on native platforms such as Android or Windows) or Indexed DB (Web/dart:html) and lazily load it into memory. No native dependencies, pure Dart code.

This repo underpins the lookup functionality of a [dictionary app](https://github.com/maxim-saplin/dikt) and thus has a number of features that make it more than just a Map serializer/deserializer:
1. All values are automatically compressed via Deflate and stored in binary shape, when requesting a value for a given key value is decompressed on the fly
2. When loading into memory (from file or IndexedDB) only keys are loaded
3. Keys are indexed which makes it possible to conduct super fast binary searches
4. There's a fast binary search implementation of keyStartingWith() which retunrs a list of matching keys
5. Lookups by key support case insensitive mode and also fast (by up-front building teh list of lower case 'shadow' keys) 
6. No arbitrary types are allowed for Keys and Values, only strings
7. The collection is optimized for fast load from storage and fast key lookup, though accessing values is outght to be infrequent and not fast (since the values are not stored in memory and are compressed and require on the ge decompression)
8. There's support for isolates, you can parallelize loading multiple IkvFiles in separate isolates for faster performance

Initially I used HiveDB to store and load key/value pairs which is fast compared to other options (also had to tweak it to allow non ASCII keys). At first it was OK, looking up among keys by iterating all of them (there're no indexed to keys and it is not possible to do binary searchs) and lazily loading boxes worked fine with dictionaries sized at hundreds of thousands of entreis and ~50MB of data. Tough when stress testing wit millions of entries and couple of gigabytes Hive became the bottleneck. Below is the speeds comparison of certain scenarios with 45MB WordNet 3 dictionary with 148730 entries (tested on dart vm):

| Use case                          | IkvPack           | Hive              |
| --------------------------------- |:-----------------:| -----------------:|
| Loading IkvPack file/Hive box     | 25.6 ms           | 732.0 ms          |
| Iterasting through all keys       | 7.3 microseconds  | 40.0 microseconds |
| Searcing for keys starting with %x| 0.5 microseconds  | 62.2 microseconds |

