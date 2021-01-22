@JS()
library bulkinsert;

import 'dart:js';
import 'dart:typed_data';
import 'package:js/js.dart';
import 'package:js/js_util.dart';

bool _bulkInsertInitialized = false;

const String _bulkInsert =
    'function bulkInsert(e,t,o){return new Promise((n,r)=>{const c=e.transaction(["keys"],"readwrite");c.oncomplete=function(){n(void 0)},c.onerror=function(e){r(e.target.error)};const s=e.transaction(["values"],"readwrite");s.oncomplete=function(){n(void 0)},s.onerror=function(e){r(e.target.error)};const u=c.objectStore("keys"),a=s.objectStore("values");for(var i=0;i<t.length;i++){a.add(o[i].buffer,i);const e=u.add(t[i],i);i==t.length-1&&(e.onsuccess=function(){kResult=e.result})}})}';

Future<void> insert(dynamic db, List<String> keys, List<Uint8List> values) {
  if (!_bulkInsertInitialized) {
    context.callMethod('eval', [_bulkInsert]);
    _bulkInsertInitialized = true;
  }

  var promise = bulkInsert(db, keys, values);

  return promiseToFuture(promise);
}

@JS()
external Object bulkInsert(
    dynamic db, List<String> keys, List<Uint8List> values);

Future<void> toFuture(Object promise) {
  return promiseToFuture(promise);
}

// function bulkInsert(db, keys, values) {
//   return new Promise((resolve, reject) => {
//     let kKesult;
//     const txKeys = db.transaction(["keys"], "readwrite");
//     txKeys.oncomplete = function() {
//       resolve(kKesult);
//     };
//     txKeys.onerror = function(event) {
//       reject(event.target.error);
//     }
//     let vResult;
//     const txValues = db.transaction(["values"], "readwrite");
//     txValues.oncomplete = function() {
//       resolve(vResult);
//     };
//     txValues.onerror = function(event) {
//       reject(event.target.error);
//     }
//     const k = txKeys.objectStore("keys");
//     const v = txValues.objectStore("values");
//     for (var i = 0; i < keys.length; i++)
//     {
//       v.add(values[i].buffer, i);
//       const request = k.add(keys[i], i);
//       if (i == keys.length-1) {
//         request.onsuccess = function() {
//            kResult = request.result;
//         }
//       }
//     }
//   });
// }