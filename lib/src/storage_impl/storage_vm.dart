import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import '../ikvpack_core.dart';

class Storage extends StorageBase {
  Storage(this._path) : _file = File(_path).openSync() {
    var f = _file as RandomAccessFile;
    f.setPositionSync(Headers.offset);
    var h = f.readSync(Headers.bytesSize);
    _headers = Headers.fromBytes(h.buffer.asByteData());
    _headers.validate(f.lengthSync());
  }

  RandomAccessFile? _file;
  bool _disposed = false;
  // List<_OffsetLength> _offsets = <_OffsetLength>[];
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

  @override
  String get path => _path;
  final String _path;

  late Headers _headers;

  @override
  Headers get headers => _headers;

  @override
  bool get noOutOfOrderFlag => _headers.noOutOfOrderFlag;
  @override
  bool get noUpperCaseFlag => _headers.noUpperCaseFlag;

  @override
  Future<IkvPackData> readSortedData(bool getShadowKeys) async {
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
    var bytes = f.readSync(_headers.shadowOffset - Headers.keysOffset).buffer;
    var bd = bytes.asByteData();
    var keys = readKeys(bd, _headers);

    if (keys.length != _headers.count) {
      throw 'Invalid file, number of keys read doesnt match number in headers';
    }

    // sw.stop();
    // print('Keys read ${sw.elapsedMicroseconds}');
    // sw.reset();
    // sw.start();

    var shadowKeys = <String>[];

    if (getShadowKeys) {
      if (headers.shadowCount > 0) {
        f.setPositionSync(_headers.shadowOffset);
        bytes =
            f.readSync(_headers.basketsOffset - _headers.shadowOffset).buffer;
        bd = bytes.asByteData();
        shadowKeys = readShadowKeys(bd, keys, _headers);
      }
    }

    f.setPositionSync(_headers.basketsOffset);
    bytes = f.readSync(_headers.offsetsOffset - _headers.basketsOffset).buffer;
    bd = bytes.asByteData();
    var baskets = readKeyBaskets(bd, _headers);

    if (baskets.length != _headers.basketsCount) {
      throw 'Invalid file, number of baskets read doesnt match number in headers';
    }

    // sw.stop();
    // print('Keys converted from UTF8 ${sw.elapsedMicroseconds}');
    // print('UTF8 converter time ${sw2.elapsedMicroseconds}');
    // sw.reset();
    // sw.start();

    //print('Creating view ${sw.elapsedMilliseconds}');

    _readValueOffsets();

    // sw.stop();
    // sw3.stop();
    // print('Offsets read ${sw.elapsedMicroseconds}');
    // print('Total read keys ${sw3.elapsedMicroseconds}');

    var data = IkvPackData(keys, shadowKeys, baskets);

    return data;
  }

  void _readValueOffsets() {
    if (_file != null) {
      _file!.setPositionSync(_headers.offsetsOffset);
      _valuesOffsets =
          _file!.readSync(_headers.count * 4 + 4).buffer.asByteData();
    }
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
    var o = index * 4;
    var offset = _valuesOffsets.getUint32(o, Endian.little);
    var offset2 = _valuesOffsets.getUint32(o + 4, Endian.little);
    var f = _file as RandomAccessFile;
    f.setPositionSync(offset);
    //var value = await f.read(length);
    var value = f.readSync(offset2 - offset);
    return value;
  }

  @override
  void reopenFile() {
    _file = File(path).openSync();
    _readValueOffsets();
  }

  @override
  int get sizeBytes => _file != null ? _file!.lengthSync() : -1;

  @override
  bool get binaryStore => true;
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

Future<void> saveToPath(String path, IkvPackData data, List<Uint8List> values,
    [Function(int progressPercent)? updateProgress]) async {
  var raf = File(path).openSync(mode: FileMode.write);
  try {
    var lengths = Uint16List(data.originalKeys.length);

    raf.setPositionSync(Headers.keysOffset + 2 * data.originalKeys.length);

    for (var i = 0; i < data.originalKeys.length; i++) {
      var line = utf8.encode(data.originalKeys[i]);
      lengths[i] = line.length;
      raf.writeFromSync(line);
    }

    var align = 4 - raf.positionSync() % 4;
    if (align != 4) {
      for (var i = 0; i < align; i++) {
        raf.writeByteSync(0);
      }
    }

    var shadowOffset = raf.positionSync();
    var basketsOffset = raf.positionSync();
    var shadowCount = 0;

    var lBytes = lengths.buffer.asUint8List();
    raf.setPositionSync(Headers.keysOffset);
    raf.writeFromSync(lBytes);

    if (data.shadowKeys.isNotEmpty) {
      var indexes = <int>[];

      for (var i = 0; i < data.originalKeys.length; i++) {
        if (data.originalKeys[i] != data.shadowKeys[i]) {
          indexes.add(i);
        }
      }

      var ll = Uint16List(indexes.length);
      var ii = Uint32List.fromList(indexes);

      shadowCount = ll.length;

      raf.setPositionSync(shadowOffset + 6 * ll.length);

      for (var i = 0; i < indexes.length; i++) {
        var line = utf8.encode(data.shadowKeys[indexes[i]]);
        ll[i] = line.length;
        raf.writeFromSync(line);
      }

      align = 2 - raf.positionSync() % 2;
      if (align != 2) {
        for (var i = 0; i < align; i++) {
          raf.writeByteSync(0);
        }
      }

      basketsOffset = raf.positionSync();

      raf.setPositionSync(shadowOffset);

      raf.writeFromSync(ii.buffer.asUint8List());
      raf.writeFromSync(ll.buffer.asUint8List());
    }

    raf.setPositionSync(basketsOffset);

    for (var b in data.keyBaskets) {
      _writeUint16(raf, b.firstLetter);
      _writeUint32(raf, b.startIndex);
    }

    align = 4 - raf.positionSync() % 4;
    if (align != 4) {
      for (var i = 0; i < align; i++) {
        raf.writeByteSync(0);
      }
    }

    var offsetsOffset = raf.positionSync();
    // move to values section start
    var valuesOffset = offsetsOffset + 4 * data.originalKeys.length + 4;

    raf.setPositionSync(valuesOffset);
    var offsets = Uint32List(values.length + 1);
    var currOffset = valuesOffset;
    var i = 1;
    offsets[0] = currOffset;

    for (var v in values) {
      raf.writeFromSync(v);
      currOffset += v.length;
      offsets[i++] = currOffset;
    }
    offsets[offsets.length - 1] = raf.lengthSync();

    raf.setPositionSync(offsetsOffset);
    raf.writeFromSync(offsets.buffer.asUint8List());

    // write headers

    var headers = Headers(0, data.originalKeys.length, shadowCount,
        shadowOffset, basketsOffset, offsetsOffset, valuesOffset);
    var bd = headers.toBytes();
    raf.setPositionSync(0);
    raf.writeFromSync(bd.buffer.asUint8List());
  } finally {
    await raf.close();
  }
}
