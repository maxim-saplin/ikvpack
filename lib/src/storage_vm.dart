import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'ikvpack.dart';

/// File layout
///
/// [Headers][Keys][Values offsets][Values]
///
/// - [Headers] section
///   - 4 bytes: reserved
///   - 4 bytes: (Length), number or key/value pairs
///   - 4 bytes: (Values offsets), location of the first byte with offsets in file and lengths in bytes of values
///   - 4 bytes: (Values start), location of the first byte with values
///   * Difference (Values start) - (Velues offset) is 8*(Length) - the length of
///   (Values offsets) section is 8 bytes time number of values.
/// - [Keys] section
///   - '\n' delimited keys
///   * Number of '\n' must be equal to (Length)
/// - [Values offsets] section
///   - Array of 8-byte pairs:
///     - 4 bytes: value offset in file
///     - 4 bytes: value length in bytes
///     * total length in bytes is equal to (File length) - (Values start)
/// - [Values] section
///   - Stream of bytes where beginning and end of certain value bytes is determind
///     by [Value offsets section]
class Storage implements StorageBase {
  Storage(this.path) : _file = File(path).openSync();

  final RandomAccessFile _file;
  bool _disposed = false;
  List<_OffsetLength> _offsets = <_OffsetLength>[];

  @override
  void dispose() {
    _file.closeSync();
    _disposed = true;
  }

  final String path;

  @override
  List<String> readSortedKeys() {
    // var sw = Stopwatch();
    // sw.start();
    // print('Reading ikvpack keys and value offsets...');
    if (_disposed) throw 'Storage object was disposed, cant use it';
    _file.setPositionSync(4); // skip reserved
    var length = _readInt32(_file);
    var offsetsOffset = _readInt32(_file);
    var valuesOffset = _readInt32(_file);
    var keys = <String>[]..length;

    if (valuesOffset - offsetsOffset != length * 8) {
      throw 'Invalid file, number of ofset entires doesn\'t match the length';
    }

    if (_file.lengthSync() <= offsetsOffset) {
      throw 'Invalid file, file to short (offsetsOffset)';
    }

    if (_file.lengthSync() <= valuesOffset) {
      throw 'Invalid file, file to short (valuesOffset)';
    }

    // tried reading file byte after byte, very slow, OS doesnt seem to read ahead and cache future file bytes
    var bytes = _file.readSync(offsetsOffset - _file.positionSync());
    // reading keys

    var prev = 0;
    var i = 0;
    keys = List.generate(length, (index) {
      while (i < bytes.length) {
        if (bytes[i] == 10) {
          var key = utf8.decode(Uint8List.view(bytes.buffer, prev, i - prev),
              allowMalformed: true);
          i++;
          prev = i;

          return key;
        }
        i++;
      }
      throw 'Invalid file, number of keys read doesnt match number in headers';
    });

    if (keys.length != length) {
      throw 'Invalid file, number of keys read doesnt match number in headers';
    }

    //print('Keys read: ${sw.elapsedMilliseconds}ms');

    // reading value offsets

    var bd = _file.readSync(length * 8).buffer.asByteData();

    _offsets = List.generate(length,
        (i) => _OffsetLength(bd.getInt32(i * 8), bd.getInt32(i * 8 + 4)));

    return keys;
  }

  /// File storage only supports referencing values by index
  @override
  List<int> value(String key) {
    throw UnimplementedError();
  }

  int _readInt32(RandomAccessFile raf) {
    var int32 = Uint8List(4);
    if (raf.readIntoSync(int32) <= 0) return -1;
    var bd = ByteData.sublistView(int32);
    var val = bd.getInt32(0);
    return val;
  }

  @override
  // TODO: implement useIndexToGetValue
  bool get useIndexToGetValue => true;

  @override
  List<int> valueAt(int index) {
    var o = _offsets[index];
    _file.setPositionSync(o.offset);
    var value = _file.readSync(o.length);
    return value.toList(growable: false);
  }
}

void saveToPath(String path, List<String> keys, List<List<int>> values) {
  void _writeInt32(RandomAccessFile raf, int value) {
    var bd = ByteData(4);
    bd.setInt32(0, value);
    raf.writeFromSync(bd.buffer.asUint8List());
  }

  var raf = File(path).openSync(mode: FileMode.write);
  try {
    raf.setPositionSync(4); //skip reserved
    _writeInt32(raf, keys.length);
    raf.setPositionSync(16); //skip offsets and values headers
    for (var k in keys) {
      var line = utf8.encode(k + '\n');
      raf.writeFromSync(line);
    }
    // write offsets posisition
    var offsetsOffset = raf.positionSync();
    raf.setPositionSync(8);
    _writeInt32(raf, offsetsOffset);

    // move to values section start
    var valuesOffset = offsetsOffset + 8 * keys.length;
    _writeInt32(raf, valuesOffset); // write values posisition
    raf.setPositionSync(valuesOffset);
    var offsets = <_OffsetLength>[];

    for (var v in values) {
      var ol = _OffsetLength(valuesOffset, v.length);
      valuesOffset += v.length;
      offsets.add(ol);
      raf.writeFromSync(v);
    }

    // write offsets
    raf.setPositionSync(offsetsOffset);
    for (var ol in offsets) {
      _writeInt32(raf, ol.offset);
      _writeInt32(raf, ol.length);
    }

    //  write values
    for (var v in values) {
      raf.writeFromSync(v);
    }
  } finally {
    raf.closeSync();
  }
}

class _OffsetLength {
  final int offset;
  final int length;
  _OffsetLength(this.offset, this.length);
}
