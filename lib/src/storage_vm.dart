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
  ByteData? _valuesOffsets;

  @override
  void dispose() {
    _file?.closeSync();
    _disposed = true;
  }

  final String path;

  int _flags = 0;
  int _length = -1;
  int _offsetsOffset = -1;
  int _valuesOffset = -1;

  @override
  bool get noOutOfOrderFlag => (_flags & 0x80000000) >> 31 == 1;
  @override
  bool get noUpperCaseFlag => (_flags & 0x80000000) >> 31 == 1;

  @override
  Future<List<String>> readSortedKeys() async {
    // var sw = Stopwatch();
    // var sw2 = Stopwatch();
    // var sw3 = Stopwatch();
    // sw3.start();
    // sw.start();
    // print('Reading ikvpack keys and value offsets...');

    var f = _file as RandomAccessFile;
    f.setPositionSync(0);

    if (_disposed) throw 'Storage object was disposed, cant use it';
    _flags = _readUint32(f, Endian.big);
    _length = _readUint32(f);
    _offsetsOffset = _readUint32(f);
    _valuesOffset = _readUint32(f);
    var keys = <String>[];

    if (_valuesOffset - _offsetsOffset != _length * 8) {
      throw 'Invalid file, number of offset entires doesn\'t match the length';
    }

    if (f.lengthSync() <= _offsetsOffset) {
      throw 'Invalid file, file to short (offsetsOffset)';
    }

    if (f.lengthSync() <= _valuesOffset) {
      throw 'Invalid file, file to short (valuesOffset)';
    }

    // sw.stop();
    // print('Init done ${sw.elapsedMicroseconds}');
    // sw.reset();
    // sw.start();

    // tried reading file byte after byte, very slow, OS doesnt seem to read ahead and cache future file bytes
    var bytes = f.readSync(_offsetsOffset - f.positionSync()).buffer;
    var bd = bytes.asByteData();
    var prev = 0;

    // sw.stop();
    // print('Keys read ${sw.elapsedMicroseconds}');
    // sw.reset();
    // sw.start();

    //var decoder = CustUtf8Decoder(true);
    var decoder = Utf8Decoder(allowMalformed: true);

    keys = List.generate(_length, (index) {
      var length = bd.getUint16(prev);
      prev += 2;
      var view = Uint8List.view(bytes, prev, length);
      var key = decoder.convert(view);

      //var key = dec.decodeGeneral(view, 0, length, true);

      prev += length;

      return key;
    }, growable: false);

    // sw.stop();
    // print('Keys converted from UTF8 ${sw.elapsedMicroseconds}');
    // print('UTF8 converter time ${sw2.elapsedMicroseconds}');
    // sw.reset();
    // sw.start();

    //print('Creating view ${sw.elapsedMilliseconds}');

    if (keys.length != _length) {
      throw 'Invalid file, number of keys read doesnt match number in headers';
    }

    _valuesOffsets = f.readSync(_length * 8).buffer.asByteData();

    // sw.stop();
    // sw3.stop();
    // print('Offsets read ${sw.elapsedMicroseconds}');
    // print('Total read keys ${sw3.elapsedMicroseconds}');

    return keys;
  }

  /// File storage only supports referencing values by index
  @override
  Uint8List value(String key) {
    throw UnimplementedError();
  }

  @override
  bool get useIndexToGetValue => true;

  @override
  Uint8List valueAt(int index) {
    //var o = _offsets[index];
    var offset = _valuesOffsets!.getUint32(index * 8);
    var length = _valuesOffsets!.getUint32(index * 8 + 4);
    var f = _file as RandomAccessFile;
    f.setPositionSync(offset);
    var value = f.readSync(length);
    return value;
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

  @override
  Future<Stats> getStats() async {
    var keys = await readSortedKeys();

    var keysNumber = _length;
    var keysBytes = _offsetsOffset - 16 - _length * 2;
    //var altKeysBytes = 0;
    var valuesBytes = sizeBytes - _valuesOffset;
    var keysTotalChars = keys.fold<int>(
        0, (previousValue, element) => previousValue + element.length);

    var distinctKeysNumber = 1;

    var prev = keys[0];
    for (var i = 1; i < keys.length; i++) {
      if (prev != keys[i]) {
        distinctKeysNumber++;
        prev = keys[i];
      }
      // altKeysBytes += keys[i].codeUnits.fold(0, (previousValue, element) {
      //   return previousValue +
      //       (element > 127
      //           ? (element > 2047 ? (element > 65535 ? 4 : 3) : 2)
      //           : 1);
      // });
    }

    var stats = Stats(
        keysNumber, distinctKeysNumber, keysBytes, valuesBytes, keysTotalChars);

    return stats;
  }
}

void deleteFromPath(String path) {
  File(path).deleteSync();
}

Future<IkvInfo> storageGetInfo(String path) async {
  var f = File(path);
  var raf = await f.open();
  await raf.setPosition(4);
  var length = await _readUint32Async(raf);
  await raf.close();
  return IkvInfo(await f.length(), length);
}

int _readUint32(RandomAccessFile raf, [Endian endian = Endian.big]) {
  var int32 = Uint8List(4);
  if (raf.readIntoSync(int32) <= 0) return -1;
  var bd = ByteData.sublistView(int32);
  var val = bd.getUint32(0, endian);
  return val;
}

Future<int> _readUint32Async(RandomAccessFile raf,
    [Endian endian = Endian.big]) async {
  var int32 = Uint8List(4);
  if (await raf.readInto(int32) <= 0) return -1;
  var bd = ByteData.sublistView(int32);
  var val = bd.getUint32(0, endian);
  return val;
}

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

Future<void> saveToPath(
    String path, List<String> keys, List<Uint8List> values) async {
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
    await raf.close();
  }
}

class _OffsetLength {
  final int offset;
  final int length;
  _OffsetLength(this.offset, this.length);
}
