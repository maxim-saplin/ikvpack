part of ikvpack_core;

/// Binary layout
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
///   - [Strings' lengths subsection]
///     - 2 byte: string length in bytes
///   - [String bytes]
///   *
/// - [Values offsets] section
///   - Array of 8-byte pairs:
///     - 4 bytes: value offset in file
///     - 4 bytes: value length in bytes
///     * total length in bytes is equal to (File length) - (Values start)
/// - [Values] section
///   - Stream of bytes where beginning and end of certain value bytes is determind
///     by [Value offsets section]

class IkvInfo {
  final int sizeBytes;
  final int keysSizeBytes;
  final int valuesSizeBytes;
  final int length;
  IkvInfo(
      this.sizeBytes, this.length, this.keysSizeBytes, this.valuesSizeBytes);
}

class Headers {
  static const int bytesSize = 16;
  static const int offset = 0;
  static const int keysOffset = 16;

  final int flags;
  final int length;
  final int offsetsOffset;
  final int valuesOffset;

  //Headers(this.flags, this.length, this.offsetsOffset, this.valuesOffset);

  Headers(ByteData data)
      : flags = data.getInt32(0),
        length = data.getInt32(4),
        offsetsOffset = data.getInt32(8),
        valuesOffset = data.getInt32(12);

  bool get noOutOfOrderFlag => (flags & 0x80000000) >> 31 == 1;

  bool get noUpperCaseFlag => (flags & 0x80000000) >> 31 == 1;

  void validate(int totalLength) {
    if (valuesOffset - offsetsOffset != length * 8) {
      throw 'Invalid data, number of offset entires doesn\'t match the length';
    }

    if (totalLength <= offsetsOffset) {
      throw 'Invalid data, to short (offsetsOffset)';
    }

    if (totalLength <= valuesOffset) {
      throw 'Invalid data, to short (valuesOffset)';
    }
  }
}

class Stats {
  final int keysNumber;
  final int distinctKeysNumber;
  final int keysBytes;
  final int valuesBytes;
  final int keysTotalChars;
  final int minKeyLength;
  final int maxKeyLengthh;

  Stats(
      this.keysNumber,
      this.distinctKeysNumber,
      this.keysBytes,
      this.valuesBytes,
      this.keysTotalChars,
      this.minKeyLength,
      this.maxKeyLengthh);

  double get avgKeyLength => keysTotalChars / keysNumber;
  double get avgKeyBytes => keysBytes / keysNumber;
  double get avgCharBytes => keysBytes / keysTotalChars;
  double get avgValueBytes => valuesBytes / keysNumber;
}

abstract class StorageBase {
  bool get noOutOfOrderFlag;
  bool get noUpperCaseFlag;

  Future<List<String>> readSortedKeys();
  Future<Uint8List> value(String key);
  Future<Uint8List> valueAt(int index);
  int get sizeBytes;
  Future<Stats> getStats();
  void dispose();
  bool get useIndexToGetValue;
  // the bellow 2 methods are workarounds for passing Storage across isolates,
  // since intenal object RandomAccessFile can't cross isolates boundaries
  // closing file is done in spawned isolate and reopening is done in main isolate
  void close();
  void reopenFile();
}

List<String> getKeys(ByteData data, Headers headers) {
  var prev = 0;
  var decoder = Utf8Decoder(allowMalformed: true);

  var keys = List<String>.generate(headers.length, (index) {
    var length = data.getUint16(prev);
    prev += 2;
    var view = Uint8List.sublistView(data, prev, prev + length);
    //var view = Uint8List.view(data.buffer, prev, length);
    var key = decoder.convert(view);
    prev += length;

    return key;
  }, growable: false);

  if (keys.length != headers.length) {
    throw 'Invalid data, number of keys read doesnt match number in headers';
  }

  return keys;
}

Tupple<List<String>, List<Uint8List>> parseBinary(ByteData data) {
  var headers = Headers(data);
  headers.validate(data.lengthInBytes);

  var keysData = data.buffer.asByteData(Headers.keysOffset);

  var keys = getKeys(keysData, headers);

  var prev = headers.offsetsOffset;

  var values = List<Uint8List>.generate(headers.length, (index) {
    var offset = data.getUint32(prev);
    prev += 4;
    var length = data.getUint32(prev);
    prev += 4;
    var view = Uint8List.view(data.buffer, offset, length);

    return Uint8List.fromList(view);
  }, growable: false);

  if (values.length != headers.length) {
    throw 'Invalid data, number of values read doesnt match number in headers';
  }

  var result = Tupple(keys, values);

  return result;
}
