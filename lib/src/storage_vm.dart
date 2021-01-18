import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'ikvpack.dart';

/// File layout
///
/// [Headers][Keys][Values offsets][Values]
///
/// - [Headers] section
///   - 4 bytes: flags
///     Bit #1 - no need to fix out-of-order chars (e.g. for non Russian languag)
///     Bit #2 - no upper-case, no need for lower-case shadow keys
///     E.g. if both bits are set there's no need for shadow keys
///   - 4 bytes: (Length), number or key/value pairs
///   - 4 bytes: (Values offsets), location of the first byte with offsets in file and lengths in bytes of values
///   - 4 bytes: (Values start), location of the first byte with values
///   * Difference (Values start) - (Velues offset) is 8*(Length) - the length of
///   (Values offsets) section is 8 bytes time number of values.
/// - [Keys] section
///   - 2 bytes: string length in bytes
///   - String bytes
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

  RandomAccessFile? _file;
  bool _disposed = false;
  //List<_OffsetLength> _offsets = <_OffsetLength>[];
  // Trading off faster dictionaru load time for slower value lookup
  // Cleaner approach with _OffsetLength list gave ~900ms load time on a large dictionary (2+ mln records)
  // Just reading data and storing ByteData gave ~660ms. It seems reasonable to avoid the delay
  // in a loop with 2mln iterations to very infrequent operastions, e.g. 0,1ms of value extraction
  // delay won't be noticed when looking up a single value (which is the more relevant case). Also given that all values are dcompresed
  // the delayed extraction of offset/length won't be comparable to the ammount of time needed by zlib
  ByteData? _offsets;

  @override
  void dispose() {
    _file?.closeSync();
    _disposed = true;
  }

  final String path;

  int _flags = 0;

  @override
  bool get noOutOfOrderFlag => (_flags & 0x80000000) >> 31 == 1;
  @override
  bool get noUpperCaseFlag => (_flags & 0x80000000) >> 31 == 1;

  @override
  List<String> readSortedKeys() {
    // var sw = Stopwatch();
    // sw.start();
    // print('Reading ikvpack keys and value offsets...');

    var f = _file as RandomAccessFile;

    if (_disposed) throw 'Storage object was disposed, cant use it';
    _flags = _readUint32(f, Endian.big);
    var length = _readUint32(f);
    var offsetsOffset = _readUint32(f);
    var valuesOffset = _readUint32(f);
    var keys = <String>[];

    if (valuesOffset - offsetsOffset != length * 8) {
      throw 'Invalid file, number of offset entires doesn\'t match the length';
    }

    if (f.lengthSync() <= offsetsOffset) {
      throw 'Invalid file, file to short (offsetsOffset)';
    }

    if (f.lengthSync() <= valuesOffset) {
      throw 'Invalid file, file to short (valuesOffset)';
    }

    // tried reading file byte after byte, very slow, OS doesnt seem to read ahead and cache future file bytes
    var bytes = f.readSync(offsetsOffset - f.positionSync()).buffer;
    var bd = bytes.asByteData();
    var prev = 0;

    // reading keys
    var decoder = Utf8Decoder(allowMalformed: true);
    keys = List.generate(length, (index) {
      var length = bd.getUint16(prev);
      prev += 2;
      var key = decoder.convert(Uint8List.view(bytes, prev, length));
      prev += length;

      return key;
    });

    if (keys.length != length) {
      throw 'Invalid file, number of keys read doesnt match number in headers';
    }

    //print('Keys read: ${sw.elapsedMilliseconds}ms');

    // reading value offsets

    //var bd = f.readSync(length * 8).buffer.asByteData();

    // _offsets = List.generate(length, (i) {
    //   // var pair = bd.getInt64(i * 8, Endian.little);
    //   // return _OffsetLength(pair >> 32, pair & 0xffffffff);
    //   return _OffsetLength(bd.getUint32(i * 8), bd.getUint32(i * 8 + 4));
    // });
    _offsets = f.readSync(length * 8).buffer.asByteData();

    return keys;
  }

  /// File storage only supports referencing values by index
  @override
  List<int> value(String key) {
    throw UnimplementedError();
  }

  int _readUint32(RandomAccessFile raf, [Endian endian = Endian.big]) {
    var int32 = Uint8List(4);
    if (raf.readIntoSync(int32) <= 0) return -1;
    var bd = ByteData.sublistView(int32);
    var val = bd.getUint32(0, endian);
    return val;
  }

  @override
  // TODO: implement useIndexToGetValue
  bool get useIndexToGetValue => true;

  @override
  List<int> valueAt(int index) {
    //var o = _offsets[index];
    var offset = _offsets!.getUint32(index * 8);
    var length = _offsets!.getUint32(index * 8 + 4);
    var f = _file as RandomAccessFile;
    f.setPositionSync(offset);
    var value = f.readSync(length);
    return value.toList(growable: false);
  }

  @override
  void closeFile() {
    _file?.close();
    _file = null;
  }

  @override
  void reopenFile() {
    _file = File(path).openSync();
  }

  @override
  int get sizeBytes => _file != null ? _file!.lengthSync() : -1;
}

void deleteFromPath(String path) {
  File(path).deleteSync();
}

void saveToPath(String path, List<String> keys, List<List<int>> values) {
  void _writeUint32(RandomAccessFile raf, int value) {
    var bd = ByteData(4);
    bd.setUint32(0, value);
    raf.writeFromSync(bd.buffer.asUint8List());
  }

  void _writeUint16(RandomAccessFile raf, int value) {
    var bd = ByteData(2);
    bd.setUint16(0, value);
    raf.writeFromSync(bd.buffer.asUint8List());
  }

  var raf = File(path).openSync(mode: FileMode.write);
  try {
    raf.setPositionSync(4); //skip reserved
    _writeUint32(raf, keys.length);
    raf.setPositionSync(16); //skip offsets and values headers
    for (var k in keys) {
      var line = utf8.encode(k);
      _writeUint16(raf, line.length);
      raf.writeFromSync(line);
    }
    // write offsets posisition
    var offsetsOffset = raf.positionSync();
    raf.setPositionSync(8);
    _writeUint32(raf, offsetsOffset);

    // move to values section start
    var valuesOffset = offsetsOffset + 8 * keys.length;
    _writeUint32(raf, valuesOffset); // write values posisition
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
      _writeUint32(raf, ol.offset);
      _writeUint32(raf, ol.length);
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
