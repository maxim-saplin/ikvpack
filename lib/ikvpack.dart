/// Indexed Key Value pack allows saving to a file a sorted key/value collection (Map<String, String>).
/// The file can be quickly loaded and provide read-only access to the collection.
/// The main idea is that keys are indexed which allows fast binary serarches among them
/// - in contrast to typical Map implementations keys are accessed via iterator.
/// Values are stored in binarry format compressed via Zlib - they are decomressed on the fly upon fetching.
///
/// Copyright (c) 2020 Maxim Saplin
library ikvpack;

export 'src/ikvpack.dart' show IkvPack;
export 'src/isolate_helpers.dart';
