import '../ikvpack.dart';

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
  IkvPack(String path) : super(path);

  IkvPack.fromStringMap(Map<String, String> map,
      [bool keysCaseInsensitive = true])
      : super.fromMap(map, keysCaseInsensitive);

  @override
  // TODO: implement indexedKeys
  bool get indexedKeys => true;

  @override
  void packToFile(String path) {
    // TODO: implement packToFile
  }
}
