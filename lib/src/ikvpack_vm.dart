import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'ikvpack_base.dart';

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
class IkvPack extends IkvPackBase {
  IkvPack(String path, [keysCaseInsensitive = true])
      : _file = File(path).openSync(),
        super(path, keysCaseInsensitive) {
    // var f = File(path);
    // var raf = f.openSync();
    if (_file == null) throw 'Error opening file ${path}';
    var f = _file as RandomAccessFile;
    f.setPositionSync(4); // skip reserved
    var length = _readInt32(f);
    var offsetsOffset = _readInt32(f);
    var valuesOffset = _readInt32(f);
    var keys = <String>[];

    if (valuesOffset - offsetsOffset != length * 8) {
      throw 'Invalid file, number of ofset entires doesn\'t match the lrngth';
    }

    //_file.readByteSync()
  }

  IkvPack.fromStringMap(Map<String, String> map, [keysCaseInsensitive = true])
      : super.fromMap(map, keysCaseInsensitive);

  RandomAccessFile? _file;
  bool _disposed = false;

  void dispose() {
    _file?.closeSync();
    _disposed = true;
  }

  int _readInt32(RandomAccessFile raf) {
    var int32 = Uint8List(4);
    if (raf.readIntoSync(int32) <= 0) return -1;
    var bd = ByteData.sublistView(int32);
    var val = bd.getInt32(0);
    return val;
  }

  @override
  // TODO: implement indexedKeys
  bool get indexedKeys => true;

  void _writeInt32(RandomAccessFile raf, int value) {
    var bd = ByteData(4);
    bd.setInt32(0, value);
    raf.writeFromSync(bd.buffer.asUint8List());
  }

  @override
  void packToFile(String path) {
    var raf = File(path).openSync(mode: FileMode.write);
    try {
      raf.setPositionSync(4); //skip reserved
      _writeInt32(raf, length);
      raf.setPositionSync(8); //skip offsets and values headers
      for (var k in keys) {
        var line = utf8.encode(k + '\n');
        raf.writeFromSync(line);
      }
      // write offsets posisition
      var offsetsOffset = raf.positionSync();
      raf.setPosition(8);
      _writeInt32(raf, offsetsOffset);

      // move to values section start
      var valuesOffset = offsetsOffset + 8 * length;
      _writeInt32(raf, valuesOffset); // write values posisition
      raf.setPosition(valuesOffset);
      var offsets = <_OffsetLength>[];

      for (var v in valuesBytes) {
        var ol = _OffsetLength(valuesOffset, v.length);
        valuesOffset += v.length;
        offsets.add(ol);
        raf.writeFromSync(v);
      }

      // write offsets
      raf.setPosition(offsetsOffset);
      for (var ol in offsets) {
        _writeInt32(raf, ol.offset);
        _writeInt32(raf, ol.length);
      }
    } finally {
      raf.closeSync();
    }
  }
}

class _OffsetLength {
  final int offset;
  final int length;
  _OffsetLength(this.offset, this.length);
}
