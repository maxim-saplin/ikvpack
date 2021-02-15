@TestOn('vm')

import 'package:ikvpack/ikvpack.dart';
import 'package:ikvpack/src/ikvpack_core.dart';
import 'package:test/test.dart';

import 'shared.dart';

void main() async {
  setUpAll(() async {
    var pool = IsolatePool(4);
    await pool.start();
    var ikv = await IkvPackProxy.loadInIsolatePoolAndUseProxy(
        pool, 'test/testIkv.dat');
    setIkv(ikv);
  });

  test('Getting range works', () async {
    var range = await ikv.getRange(1, 11);
    expect(range.length, 11);
    var x = range.entries.first;
    expect(x.key, 'Aaron Burr');
    expect(x.value,
        '<div><i>noun</i></div><div>United States politician who served as vice president under Jefferson; he mortally wounded his political rival Alexander Hamilton in a duel and fled south <i>(1756-1836)</i></div><div><span>•</span> <i>Syn</i>: ↑<a href=Burr>Burr</a></div><div><span>•</span> <i>Instance Hypernyms</i>: ↑<a href=politician>politician</a>, ↑<a href=politico>politico</a>, ↑<a href=pol>pol</a>, ↑<a href=political leader>political leader</a></div>');
  });
}
