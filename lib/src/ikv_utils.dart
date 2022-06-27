import 'dart:io';
import 'dart:typed_data';
import 'package:path/path.dart' as path;

const blockSize = 4 * 1024;
const partString = '.part';
const maxCount = 1024;

class MergedFileHeaders {
// File outline
// [Files' data]
// [Offsets, 8 byte in each]
// [Count - number of individual files in this container file, 8 bytes]
// [File marker used to tell the file is created by this util, 8 byte int]

  static const int fileMarker = 0x0033002200110022;
  late final int count;
  late final List<int> offsets;

  MergedFileHeaders(this.count, this.offsets);

  MergedFileHeaders.fromFile(RandomAccessFile file) {
    var length = file.lengthSync();
    if (length < 16) {
      throw 'Invalid file, too short, at least 16 bytes expected for headers';
    }

    file.setPositionSync(length - 16);
    var sixtenBytes = file.readSync(16).buffer.asByteData();
    var marker = sixtenBytes.getUint64(8, Endian.little);

    if (marker != fileMarker) {
      throw 'The file appers to be of an unsupported format';
    }

    count = sixtenBytes.getUint64(0, Endian.little);
    if (count > maxCount) {
      throw 'Too many items $count, maximum allowd is $maxCount';
    }
    if (count * 8 + 16 > length) {
      throw 'Invalid file, too short, the reported count is more than the number of offsets that can fit';
    }

    offsets = List<int>.filled(count, 0);

    file.setPositionSync(length - 16 - count * 8);

    for (var i = 0; i < count; i++) {
      var offset =
          file.readSync(8).buffer.asByteData().getUint64(0, Endian.little);
      offsets[i] = offset;
    }
  }

  Uint8List toBytes() {
    if (count <= 0) return Uint8List(0);

    var data = Uint64List(count + 2);
    var i = 0;

    for (i = 0; i < count; i++) {
      data[i] = offsets[i];
    }

    data[i] = count;
    data[i + 1] = fileMarker;

    return data.buffer.asUint8List();
  }

  int get sizeInBytes {
    return 16 + count * 8;
  }
}

/// Take several binary files (presumably Ikv packs though the routine doesn't care)
/// and merge them into a single one. Add headers to allow [extractFromSingleFile]
/// determine files locations and to the reverse operation.
void putIntoSingleFile(Iterable<String> files, String outputFile) {
  if (files.length > maxCount) {
    throw 'Too many items ${files.length}, maximum allowd is $maxCount';
  }
  var raf = File(outputFile).openSync(mode: FileMode.write);
  var header = MergedFileHeaders(files.length, List.filled(files.length, 0));
  var buffer = Uint8List(blockSize);
  var n = 0;
  try {
    for (var file in files) {
      header.offsets[n] = raf.positionSync();
      var f = File(file).openSync(mode: FileMode.read);
      try {
        int blocks = f.lengthSync() ~/ blockSize;
        int remainingBytes = f.lengthSync() % blockSize;
        for (var i = 0; i < blocks; i++) {
          f.readIntoSync(buffer);
          raf.writeFromSync(buffer);
        }
        if (remainingBytes > 0) {
          f.readIntoSync(buffer, 0, remainingBytes);
          raf.writeFromSync(buffer, 0, remainingBytes);
        }
      } finally {
        f.closeSync();
      }
      n++;
    }
    raf.writeFromSync(header.toBytes());
  } finally {
    raf.closeSync();
  }
}

/// Takes as an input the merged by [putIntoSingleFile] file and extracts them into
/// the given folder with the names following the convestion (fileName).part{x}.(extension)
/// where {x} is the ordinal number, (extension is provided in the given class)
void extractFromSingleFile(String filePath, String outputDir,
    [String extension = '.ikv']) {
  var raf = File(filePath).openSync(mode: FileMode.read);
  try {
    var header = MergedFileHeaders.fromFile(raf);
    raf.setPositionSync(0);
    var name = path.basenameWithoutExtension(filePath);
    var buffer = Uint8List(blockSize);

    for (var i = 0; i < header.count; i++) {
      var fn = path.join(
          outputDir, name + partString + (i + 1).toString() + extension);
      var f = File(fn).openSync(mode: FileMode.write);
      try {
        //var start = header.offsets[i];
        var length = i < header.offsets.length - 1
            ? header.offsets[i + 1] - header.offsets[i]
            : raf.lengthSync() - header.sizeInBytes;

        int blocks = length ~/ blockSize;
        int remainingBytes = length % blockSize;
        for (var i = 0; i < blocks; i++) {
          raf.readIntoSync(buffer);
          f.writeFromSync(buffer);
        }
        if (remainingBytes > 0) {
          raf.readIntoSync(buffer, 0, remainingBytes);
          f.writeFromSync(buffer, 0, remainingBytes);
        }
      } finally {
        f.closeSync();
      }
    }
  } finally {
    raf.closeSync();
  }
}
