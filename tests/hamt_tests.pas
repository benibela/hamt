program hamt_tests;

{$mode objfpc}{$H+}{$ModeSwitch typehelpers}

uses
  {$IFDEF UNIX}{$IFDEF UseCThreads}
  cthreads,
  {$ENDIF}{$ENDIF}
  Classes, bbhamt, commontestutils, sysutils, bbutils, strutils, contnrs
  { you can add units after this };



type TMutableMap_Test = class(TMutableMapStringString) //object(specialize THAMT<string, string, THAMTTypeInfo>)
  procedure testInsert(const k, v: string; override: boolean = false);
  procedure testGet(const k, v: string);
  procedure testRemove(const k: string; notthere: boolean = false);
  procedure testEnumeration(expectedCount: integer);
end;

const MISSING = 'MISSING';



procedure TMutableMap_Test.testInsert(const k, v: string; override: boolean);
var c: integer;
begin
  c := count;
  test(insert(k, v) xor override, 'insert failed (override marker?)');
  test(contains(k), 'insert failed: ' + k);
  test(get(k, 'xx'), v);
  if not override then inc(c);
  test(count, c);
end;

procedure TMutableMap_Test.testGet(const k, v: string);
begin
  test(get(k, MISSING), v, 'get ' + k);
end;

procedure TMutableMap_Test.testRemove(const k: string; notthere: boolean);
var c: integer;
begin
  c := count;
  test(remove(k) <> notthere, 'remove failed: ' + k);
  test(get(k, MISSING), MISSING);
  if not notthere then dec(c);
  test(count, c);
end;

procedure TMutableMap_Test.testEnumeration(expectedCount: integer);
var
  pair: TMutableMap_Test.PPair;
  visitedKeys: TFPStringHashTable;
  acount: integer;
begin
  acount := 0;
  visitedKeys := TFPStringHashTable.Create;
  for pair in self do begin
    test(visitedKeys.Find(pair^.key) = nil, 'duplicated key');
    inc(acount);
    test(get(pair^.key, MISSING), pair^.value);
    visitedKeys.Add(pair^.key, pair^.value);
  end;
  test(acount, expectedCount);
  visitedKeys.Free;
end;

function testInsert(m: TImmutableMapStringString; const k, v: string; override: boolean = false): TImmutableMapStringString;
var c: integer;
begin
  c := m.count;
  result := m.insert(k, v);
//  test( xor override, 'insert failed (override marker?)');
  test(result.contains(k), 'insert failed: ' + k);
  test(result.get(k, 'xx'), v);
  test(m.count, c);
  if not override then
    inc(c);
  test(result.count, c);
end;

procedure testEnumeration(m: TImmutableMapStringString; expectedCount: integer);
var
  pair: TMutableMap_Test.PPair;
  visitedKeys: TFPStringHashTable;
  acount: integer;
begin
  acount := 0;
  visitedKeys := TFPStringHashTable.Create;
  for pair in m do begin
    test(visitedKeys.Find(pair^.key) = nil, 'duplicated key');
    inc(acount);
    test(m.get(pair^.key, MISSING), pair^.value);
    visitedKeys.Add(pair^.key, pair^.value);
  end;
  test(acount, expectedCount);
  visitedKeys.Free;
end;


var
  hamt, hamt2: TMutableMap_Test;
  imap, imap2, imap3, imap4: TImmutableMapStringString;
begin
  hamt := TMutableMap_Test.create;
  hamt.testInsert('hello', 'world');
  test(not hamt.contains('xyz'));
  hamt.testInsert('foo', 'bar');

  hamt.testGet('hello', 'world');
  hamt.testGet('foo', 'bar');

  hamt.testInsert('hello', 'override', true);

  hamt.testGet('foo', 'bar');

  hamt.testEnumeration(2);
  hamt.remove('hello');
  hamt.free;


  //test collisions
  hamt := TMutableMap_Test.create;
  hamt.testInsert('test', 'A');
  hamt.testInsert('collision', 'B');
  hamt.testInsert('collision+1', 'C');
  hamt.testInsert('collision+2', 'D');

  hamt.testGet('test', 'A');
  hamt.testGet('collision', 'B');
  hamt.testGet('collision+1', 'C');
  hamt.testGet('collision+2', 'D');
  hamt.testEnumeration(4);
  hamt.testRemove('test');
  hamt.testRemove('test', true);
  hamt.testRemove('test!', true);
  hamt.free;


  hamt := TMutableMap_Test.create;
  hamt.testInsert('_00_01_02', 'x1');
  hamt.testInsert('_00_01_02', 'x2', true);
  hamt.testInsert('_00_02_03', 'x3');

  hamt.testGet('_00_01_02', 'x2');
  hamt.testGet('_00_02_03', 'x3');
  hamt.testEnumeration(2);
  hamt.testRemove('_01_02-03', true);
  hamt.free;

  hamt := TMutableMap_Test.create;
  hamt.testInsert('_00_01_02', 'x1');
  hamt.testInsert('_00_01-02', 'x1b');
  hamt.testInsert('_00_01-03', 'y');
  hamt.testGet('_00_01_02', 'x1');
  hamt.testGet('_00_01-02', 'x1b');
  hamt.testGet('_00_01-03', 'y');
  hamt.testEnumeration(3);
  hamt.testRemove('_01_01-02', true);
  hamt.testRemove('_01_02-02', true);
  hamt.free;

  //test prefix collisions
  hamt := TMutableMap_Test.create;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02_01_00', 'x2', true);
  hamt.testInsert('_03_02_00', 'x3');

  hamt.testGet('_02_01_00', 'x2');
  hamt.testGet('_03_02_00', 'x3');
  hamt.testEnumeration(2);
  hamt.free;

  hamt := TMutableMap_Test.create;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02x01_00', 'x2');
  hamt.testInsert('_03_02_00', 'x3');

  hamt.testGet('_02_01_00', 'x1');
  hamt.testGet('_02x01_00', 'x2');
  hamt.testGet('_03_02_00', 'x3');
  hamt.testEnumeration(3);
  hamt.free;


  //test snapshots
  hamt := TMutableMap_Test.create;
  hamt.testInsert('hello', 'world');
  hamt.testInsert('foo', 'bar');
  hamt2 := TMutableMap_Test(hamt.clone);
  hamt.testInsert('hello', 'override', true);

  hamt.testGet('hello', 'override');
  hamt.testGet('foo', 'bar');

  hamt2.testGet('hello', 'world');
  hamt2.testGet('foo', 'bar');
  hamt.testEnumeration(2);
  hamt2.testEnumeration(2);
  hamt.free;
  hamt2.Free;


  hamt := TMutableMap_Test.create;
  hamt.testInsert('hello', 'world');
  hamt.testInsert('foo', 'bar');
  hamt2 := TMutableMap_Test(hamt.clone);
  hamt.testInsert('new', 'N');

  hamt.testGet('hello', 'world');
  hamt.testGet('foo', 'bar');
  hamt.testGet('new', 'N');

  hamt2.testGet('hello', 'world');
  hamt2.testGet('foo', 'bar');
  hamt2.testGet('new', MISSING);
  hamt.testEnumeration(3);
  hamt2.testEnumeration(2);
  hamt.testRemove('new');
  hamt2.testRemove('hello');
  hamt.testEnumeration(2);
  hamt2.testEnumeration(1);
  hamt.free;
  hamt2.free;


  hamt := TMutableMap_Test.create;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02_01_00', 'x2', true);
  hamt2 := TMutableMap_Test(hamt.clone);
  hamt.testInsert('_03_02_00', 'x3');

  hamt.testGet('_02_01_00', 'x2');
  hamt.testGet('_03_02_00', 'x3');
  hamt2.testGet('_02_01_00', 'x2');
  hamt2.testGet('_03_02_00', MISSING);

  hamt.testInsert('_03_03_00', 'x4');
  hamt2.free;
  hamt2 := TMutableMap_Test(hamt.clone);
  hamt.testInsert('_03_03_00', 'x5', true);

  hamt.testGet('_02_01_00', 'x2');
  hamt.testGet('_03_02_00', 'x3');
  hamt.testGet('_03_03_00', 'x5');
  hamt2.testGet('_02_01_00', 'x2');
  hamt2.testGet('_03_02_00', 'x3');
  hamt2.testGet('_03_03_00', 'x4');

  hamt.testEnumeration(3);
  hamt2.testEnumeration(3);
  hamt.free;
  hamt2.free;



  hamt := TMutableMap_Test.create;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02-01_00', 'x2');
  hamt2 := TMutableMap_Test(hamt.clone);
  hamt.testInsert('_03_02_00', 'x3');

  hamt.testGet('_02_01_00', 'x1');
  hamt.testGet('_02-01_00', 'x2');
  hamt.testGet('_03_02_00', 'x3');
  hamt2.testGet('_02_01_00', 'x1');
  hamt2.testGet('_02-01_00', 'x2');
  hamt2.testGet('_03_02_00', MISSING);
  hamt.testEnumeration(3);
  hamt2.testEnumeration(2);
  hamt.testRemove('_02_01_00');
  hamt.testEnumeration(2);
  hamt2.testEnumeration(2);
  hamt.testRemove('_02-01_00');
  hamt.testEnumeration(1);
  hamt2.testEnumeration(2);
  hamt.free;
  hamt2.free;



  hamt := TMutableMap_Test.create;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02-01_00', 'x2');
  hamt2 := TMutableMap_Test(hamt.clone);
  hamt.testInsert('_02x01_00', 'x3');

  hamt.testGet('_02_01_00', 'x1');
  hamt.testGet('_02-01_00', 'x2');
  hamt.testGet('_02x01_00', 'x3');
  hamt2.testGet('_02_01_00', 'x1');
  hamt2.testGet('_02-01_00', 'x2');
  hamt2.testGet('_02x01_00', MISSING);
  hamt.testEnumeration(3);
  hamt2.testEnumeration(2);
  hamt.remove('_02_01_00');
  hamt.remove('_02-01_00');
  hamt.insert('_31_31_00', 'xy');
  hamt.remove('_02x01_00');
  hamt.remove('_31_31_00');
  hamt.free;
  hamt2.free;


  hamt := TMutableMap_Test.create;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02-01_00', 'x2');
  hamt2 := TMutableMap_Test(hamt.clone);
  hamt.testInsert('_02-01_00', 'x3', true);

  hamt.testGet('_02_01_00', 'x1');
  hamt.testGet('_02-01_00', 'x3');
  hamt2.testGet('_02_01_00', 'x1');
  hamt2.testGet('_02-01_00', 'x2');
  hamt.testEnumeration(2);
  hamt2.testEnumeration(2);

  hamt.testRemove('_02_01_00');
  hamt.testRemove('_02-01_00');
  hamt2.testRemove('_02-01_00');
  hamt.testEnumeration(0);
  hamt2.testEnumeration(1);

  hamt.free;
  hamt2.free;


  //more remove tests
  hamt := TMutableMap_Test.create;
  hamt.testInsert('_02_00_31', 'x1');
  hamt.testInsert('_03-00_31', 'x2');
  hamt2 := TMutableMap_Test(hamt.clone);
  hamt.testRemove('_03_00_31', true);
  hamt.testRemove('_03-00_31');
  hamt2.testEnumeration(2);
  hamt.testRemove('_02_00_31');
  hamt.testEnumeration(0);
  hamt2.testEnumeration(2);
  hamt.free;
  hamt2.free;

  hamt := TMutableMap_Test.create;
  hamt.testInsert('_00_00_00', 'x1');
  hamt.testInsert('_31-31_31', 'x2');
  hamt.testInsert('_30-31_31', 'x3');
  hamt.testEnumeration(3);
  hamt.testRemove('_31-31_31');
  hamt.testEnumeration(2);
  hamt.testRemove('_30-31_31');
  hamt.testEnumeration(1);
  hamt.free;

  hamt := TMutableMap_Test.create;
  hamt.testInsert('_31-31_31', 'x1');
  hamt.testInsert('_31x31_31', 'x2');
  hamt.testInsert('_00x00_01', 'x3');
  hamt.testRemove('_31-31_31'); //makes _31x31_31 into an array
  hamt2 := TMutableMap_Test(hamt.clone);
  hamt.testRemove('_31x31_31');
  hamt.free;
  hamt2.free;



  //immutable interface
  imap := TImmutableMapStringString.Create;
  imap2 := testInsert(imap, 'a', 'x');
  imap3 := testInsert(imap, 'b', 'y');
  testEnumeration(imap2, 1);
  testEnumeration(imap3, 1);
  imap4 := testInsert(imap3, 'b', 'z', true);
  testEnumeration(imap4, 1);
  imap4.free;
  imap4 := testInsert(imap2, 'c', 'z');
  testEnumeration(imap4, 2);
  imap4.free;
  imap4 := imap2.remove('a');
  testEnumeration(imap4, 0);
  imap4.free;
  imap.Free;
  imap2.Free;
  imap3.Free;
end.

