import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import '../ikvpack_core.dart';

class Storage extends StorageBase {
  Storage(this.path) : _file = File(path).openSync() {
    var f = _file as RandomAccessFile;
    f.setPositionSync(Headers.offset);
    var h = f.readSync(Headers.bytesSize);
    headers = Headers(h.buffer.asByteData());
    headers.validate(f.lengthSync());
  }

  RandomAccessFile? _file;
  bool _disposed = false;
  //List<_OffsetLength> _offsets = <_OffsetLength>[];
  // Trading off faster dictionaru load time for slower value lookup
  // Cleaner approach with _OffsetLength list gave ~900ms load time on a large dictionary (2+ mln records)
  // Just reading data and storing ByteData gave ~660ms. It seems reasonable to avoid the delay
  // in a loop with 2mln iterations to very infrequent operastions, e.g. 0,1ms of value extraction
  // delay won't be noticed when looking up a single value (which is the more relevant case). Also given that all values are dcompresed
  // the delayed extraction of offset/length won't be comparable to the ammount of time needed by zlib
  late ByteData _valuesOffsets;

  @override
  void dispose() {
    close();
    _disposed = true;
  }

// Workaround for Isolates
  @override
  void close() {
    _file?.close();
    _file = null;
  }

  final String path;

  late Headers headers;

  @override
  bool get noOutOfOrderFlag => headers.noOutOfOrderFlag;
  @override
  bool get noUpperCaseFlag => headers.noUpperCaseFlag;

  @override
  Future<List<String>> readSortedKeys() async {
    // var sw = Stopwatch();
    // var sw2 = Stopwatch();
    // var sw3 = Stopwatch();
    // sw3.start();
    // sw.start();
    // print('Reading ikvpack keys and value offsets...');

    if (_disposed) throw 'Storage object was disposed, cant use it';

    // _flags = _readUint32(f, Endian.big);
    // _length = _readUint32(f);
    // _offsetsOffset = _readUint32(f);
    // _valuesOffset = _readUint32(f);

    var f = _file as RandomAccessFile;

    // sw.stop();
    // print('Init done ${sw.elapsedMicroseconds}');
    // sw.reset();
    // sw.start();

    // tried reading file byte after byte, very slow, OS doesnt seem to read ahead and cache future file bytes
    f.setPositionSync(Headers.keysOffset);
    var bytes = f.readSync(headers.offsetsOffset - Headers.keysOffset).buffer;
    var bd = bytes.asByteData();

    // sw.stop();
    // print('Keys read ${sw.elapsedMicroseconds}');
    // sw.reset();
    // sw.start();

    f.setPositionSync(Headers.keysOffset);
    var keys = getKeys(bd, headers);

    // sw.stop();
    // print('Keys converted from UTF8 ${sw.elapsedMicroseconds}');
    // print('UTF8 converter time ${sw2.elapsedMicroseconds}');
    // sw.reset();
    // sw.start();

    //print('Creating view ${sw.elapsedMilliseconds}');

    // if (keys.length != _length) {
    //   throw 'Invalid file, number of keys read doesnt match number in headers';
    // }

    f.setPositionSync(headers.offsetsOffset);
    _valuesOffsets = f.readSync(headers.length * 8).buffer.asByteData();

    // sw.stop();
    // sw3.stop();
    // print('Offsets read ${sw.elapsedMicroseconds}');
    // print('Total read keys ${sw3.elapsedMicroseconds}');

    return keys;
  }

  /// File storage only supports referencing values by index
  @override
  Future<Uint8List> value(String key) {
    throw UnimplementedError();
  }

  @override
  bool get useIndexToGetValue => true;

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  @override
  Future<Uint8List> valueAt(int index) async {
    if (_disposed) throw 'Storage object was disposed, cant use it';
    var o = index * 8;
    var offset = _valuesOffsets.getUint32(o);
    var length = _valuesOffsets.getUint32(o + 4);
    var f = _file as RandomAccessFile;
    f.setPositionSync(offset);
    //var value = await f.read(length);
    var value = f.readSync(length);
    return value;
  }

  @override
  void reopenFile() {
    _file = File(path).openSync();
  }

  @override
  int get sizeBytes => _file != null ? _file!.lengthSync() : -1;

  @override
  Future<Stats> getStats() async {
    if (_disposed) throw 'Storage object was disposed, cant use it';
    var keys = await readSortedKeys();

    var keysNumber = headers.length;
    var keysBytes = headers.offsetsOffset - 16 - headers.length * 2;
    //var altKeysBytes = 0;
    var valuesBytes = sizeBytes - headers.valuesOffset;
    var keysTotalChars = keys.fold<int>(
        0, (previousValue, element) => previousValue + element.length);

    var distinctKeysNumber = 1;

    var minKeyLength = 1000000;
    var maxKeyLength = 0;

    var prev = keys[0];
    for (var i = 1; i < keys.length; i++) {
      if (keys[i].length > maxKeyLength) maxKeyLength = keys[i].length;
      if (keys[i].length < minKeyLength) minKeyLength = keys[i].length;
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

    var stats = Stats(keysNumber, distinctKeysNumber, keysBytes, valuesBytes,
        keysTotalChars, minKeyLength, maxKeyLength);

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
  return IkvInfo(await f.length(), length, 0, 0);
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

Future<void> saveToPath(String path, List<String> keys, List<Uint8List> values,
    [Function(int progressPercent)? updateProgress]) async {
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
