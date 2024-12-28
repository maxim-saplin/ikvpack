part of 'ikvpack_core.dart';

/// Binary layout
///
/// [Headers][Keys][Values offsets][Values]
///
/// - [Headers] section
///   - 4 bytes: flags
///     Bit #1 - no need to fix out-of-order chars (e.g. for non Russian languag)
///     Bit #2 - no upper-case, no need for lower-case shadow keys
///     E.g. if both bits are set there's no need for shadow keys
///   - 4 bytes: (Count), number or key/value pairs
///   - 4 bytes: (Shadow keys count), number of shadow keys (only those are saved which are different from original keys)
///   - 4 bytes: (Shadow keys offset), first byte of shadow keys
///   - 4 bytes: (Key Baskets offset), first byte of keys' baskets
///   - 4 bytes: (Values offsets), location of the first byte with offsets in file and lengths in bytes of values
///   - 4 bytes: (Values start), location of the first byte with values
/// - [Keys] section - starts right after headers
///   - [Strings' lengths subsection]
///     - 2 byte array - string lengths in bytes
///   - [String bytes] - starts at [Keys] section  + (Count) * 2
///   - Alignment to 4 bytes - add empty bytes if start of next section happens to be not multiple of 4, byte alignment is required by Uint32List.sublistView() if reading file as a whole and sublisting on this big BinaryData and using IkvMap.fromBytes()
/// - [Shadow keys] section
///   - [Strings' indexes subsection]
///    - 4 byte array - indexes of linked [Keys]
///   - [Strings' lengths subsection]
///    - 2 byte array - string lengths in bytes
///   - [String bytes] - starts at [Shadow keys] section  + (Count) * 6
///   - Alignment to 2 bytes
/// - [Key Baskets]
///   - 2 bytes: char code unit
///   - 4 bytes: first index of key starting with char code
///   - Alignment to 4 bytes - add empty bytes if start of next section happens to be not multiple of 4, byte alignment is required by Uint32List.sublistView
/// - [Values offsets] section
///   - Array of 4-byte pairs:
///     - 4 bytes: value offset in file
///   * Extra 4 bytes with no real value offset is added for simplier calculation of value length: length of x is offset of x+1 - offset of x
/// - [Values] section
///   - Stream of bytes where beginning and end of certain value bytes is determind
///     by [Value offsets] section

class IkvInfo {
  final int sizeBytes;
  final int count;
  IkvInfo(this.sizeBytes, this.count);
}

class Headers {
  static const int bytesSize = 28;
  static const int offset = 0;

  final int flags;
  final int count;
  static const int keysOffset = bytesSize;
  int get keysBytes => keysOffset + 2 * count;
  final int shadowCount;
  final int shadowOffset;
  int get shadowBytes => shadowOffset + 6 * shadowCount;
  final int basketsOffset;
  int get basketsCount => ((offsetsOffset - basketsOffset) / 6).round();
  final int offsetsOffset;
  final int valuesOffset;

  Headers(this.flags, this.count, this.shadowCount, this.shadowOffset,
      this.basketsOffset, this.offsetsOffset, this.valuesOffset);

  Headers.fromBytes(ByteData data)
      : flags = data.getInt32(0),
        count = data.getInt32(4),
        shadowCount = data.getInt32(8),
        shadowOffset = data.getInt32(12),
        basketsOffset = data.getInt32(16),
        offsetsOffset = data.getInt32(20),
        valuesOffset = data.getInt32(24);

  ByteData toBytes() {
    var bd = ByteData(28);
    bd.setInt32(0, flags);
    bd.setInt32(4, count);
    bd.setInt32(8, shadowCount);
    bd.setInt32(12, shadowOffset);
    bd.setInt32(16, basketsOffset);
    bd.setInt32(20, offsetsOffset);
    bd.setInt32(24, valuesOffset);
    return bd;
  }

  bool get noOutOfOrderFlag => (flags & 0x80000000) >> 31 == 1;

  bool get noUpperCaseFlag => (flags & 0x80000000) >> 31 == 1;

  void validate(int totalLength) {
    if (valuesOffset - offsetsOffset != (count + 1) * 4) {
      throw 'Invalid data, number of offset entires doesn\'t match the length';
    }

    if (!(((offsetsOffset - basketsOffset) % 6 == 2) ||
        (offsetsOffset - basketsOffset) % 6 == 0)) {
      throw 'Invalid data, key basket section corrupt';
    } // alignment is possble, extra 2 bytes might appear

    if (totalLength <= offsetsOffset) {
      throw 'Invalid data, to short (offsetsOffset)';
    }

    if (totalLength <= valuesOffset) {
      throw 'Invalid data, to short (valuesOffset)';
    }
  }
}

class IkvPackData {
  final List<String> originalKeys;
  final List<String> shadowKeys;
  final List<KeyBasket> keyBaskets;

  IkvPackData(this.originalKeys, this.shadowKeys, this.keyBaskets) {
    if (shadowKeys.isNotEmpty && shadowKeys.length != originalKeys.length) {
      throw 'Shadow keys length to same as Original keys list';
    }
  }
}

abstract class StorageBase {
  bool get noOutOfOrderFlag;
  bool get noUpperCaseFlag;

  String get path;
  Future<IkvPackData> readSortedData(bool getShadowKeys);
  Future<Uint8List> value(String key);
  Future<Uint8List> valueAt(int index);
  int get sizeBytes;
  void dispose();
  bool get useIndexToGetValue;
  bool get binaryStore;
  Headers get headers;
  // the bellow 2 methods are workarounds for passing Storage across isolates,
  // since intenal object RandomAccessFile can't cross isolates boundaries
  // closing file is done in spawned isolate and reopening is done in main isolate
  void close();
  void reopenFile();
}

List<String> readKeys(ByteData data, Headers headers) {
  var decoder = Utf8Decoder(allowMalformed: true);

  //var lengths = data.buffer.asUint16List(0, headers.count);
  var lengths = Uint16List.sublistView(data, 0, headers.count * 2);
  var prev = 2 * headers.count;

  var keys = List<String>.generate(headers.count, (index) {
    var view = Uint8List.sublistView(data, prev, prev + lengths[index]);
    //var view = Uint8List.view(data.buffer, prev, length);
    var key = decoder.convert(view);
    prev += lengths[index];

    return key;
  }, growable: false);

  if (keys.length != headers.count) {
    throw 'Invalid data, number of keys read doesnt match number in headers';
  }

  return keys;
}

List<String> readShadowKeys(ByteData data, List<String> keys, Headers headers) {
  if (headers.shadowCount < 1) return [];

  var decoder = Utf8Decoder(allowMalformed: true);

  var shadowKeys = List<String>.filled(keys.length, '');

  var indexes = Uint32List.sublistView(data, 0, headers.shadowCount * 4);
  var prev = headers.shadowCount * 2 + headers.shadowCount * 4;
  var lengths = Uint16List.sublistView(data, headers.shadowCount * 4, prev);

  for (var i = 0; i < lengths.length; i++) {
    var view = Uint8List.sublistView(data, prev, prev + lengths[i]);
    var key = decoder.convert(view);
    shadowKeys[indexes[i]] = key;
    prev += lengths[i];
  }

  for (var i = 0; i < keys.length; i++) {
    if (shadowKeys[i] == '') shadowKeys[i] = keys[i];
  }

  return shadowKeys;
}

List<KeyBasket> readKeyBaskets(ByteData data, Headers headers) {
  var prev = 0;

  var baskets = List<KeyBasket>.generate(headers.basketsCount, (index) {
    var cu = data.getUint16(prev);
    prev += 2;
    var i = data.getUint32(prev);
    prev += 4;
    var b = KeyBasket(cu, i, 0);
    return b;
  }, growable: false);

  for (var i = 0; i < baskets.length - 1; i++) {
    baskets[i]._endIndex = baskets[i + 1].startIndex - 1;
  }

  baskets[baskets.length - 1]._endIndex = headers.count - 1;

  return baskets;
}

Tuple<IkvPackData, List<Uint8List>> parseBinary(
    ByteData data, bool getShadowKeys) {
  var headers = Headers.fromBytes(data);
  headers.validate(data.lengthInBytes);

  var d = data.buffer.asByteData(Headers.keysOffset);
  var keys = readKeys(d, headers);

  var shadowKeys = <String>[];
  if (getShadowKeys) {
    d = data.buffer.asByteData(headers.shadowOffset);
    shadowKeys = readShadowKeys(d, keys, headers);
  }

  d = data.buffer.asByteData(headers.basketsOffset);
  var baskets = readKeyBaskets(d, headers);

  var prev = headers.offsetsOffset;

  var values = List<Uint8List>.generate(headers.count, (index) {
    var offset1 = data.getUint32(prev, Endian.little);
    prev += 4;
    var offset2 = data.getUint32(prev, Endian.little);
    var view = Uint8List.view(data.buffer, offset1, offset2 - offset1);

    return Uint8List.fromList(view);
  }, growable: false);

  if (values.length != headers.count) {
    throw 'Invalid data, number of values read doesnt match number in headers';
  }

  var result = Tuple(IkvPackData(keys, shadowKeys, baskets), values);

  return result;
}

class Stats {
  final int keysNumber;
  final int distinctKeysNumber;
  final int shadowKeysDifferentFromOrigNumber;
  final int keysBytes;
  final int valuesBytes;
  // keys + values + overhead
  final int totalBytes;
  final int keysTotalChars;
  final int minKeyLength;
  final int maxKeyLength;

  Stats(
      this.keysNumber,
      this.distinctKeysNumber,
      this.shadowKeysDifferentFromOrigNumber,
      this.keysBytes,
      this.valuesBytes,
      this.totalBytes,
      this.keysTotalChars,
      this.minKeyLength,
      this.maxKeyLength);

  double get avgKeyLength => keysTotalChars / keysNumber;
  double get avgKeyBytes => keysBytes / keysNumber;
  double get avgCharBytes => keysBytes / keysTotalChars;
  double get avgValueBytes => valuesBytes / keysNumber;
}
