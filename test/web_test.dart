@TestOn('chrome')
import 'package:ikvpack/ikvpack.dart';
import 'package:test/test.dart';

void main() async {
  test('Same read back', () async {
    var m = <String, String>{'wew': 'dsdsd', 'sdss': 'd'};
    var ik = IkvPack.fromMap(m);
    await ik.saveTo('test');
    ik = await IkvPack.load('test');
    expect(ik.keys[0], 'sdss');
    expect(ik.keys[1], 'wew');
    expect(ik.length, 2);
  });
}
