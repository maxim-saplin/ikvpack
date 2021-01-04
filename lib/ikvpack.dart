/// Indexed Key Value pack allows saving to a file a sorted key/value collection (Map<String, UNit8List>).
/// The file can be quickly loaded and provide read-only access to the collection.
/// The main idea is that keys are indexed which allows fast binary serarches among them
/// - in contrast to typical Map implementations keys are accessed via iterator.
///
/// Copyright (c) 2020 Maxim Saplin
library ikvpack;

export 'src/ikvpack_base.dart';
export 'src/ikvpack_vm.dart';
export 'src/ikvpack_web.dart';

// TODO: Export any libraries intended for clients of this package.
