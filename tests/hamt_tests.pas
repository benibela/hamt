program hamt_tests;

{$mode objfpc}{$H+}{$ModeSwitch typehelpers}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  Classes, hamt.internals, commontestutils, sysutils, bbutils, contnrs, hamt.maps, hamt.sets
  { you can add units after this };


type THAMTTestTypeInfo = object(THAMTTypeInfo)
  class function hash(const s: string): THAMTHash;
end;

type TMutableMap_Test = class(specialize TMutableMap<string, string, THAMTTestTypeInfo>)
  procedure testInsert(const k, v: string; override: boolean = false);
  procedure testGet(const k, v: string);
  procedure testRemove(const k: string; notthere: boolean = false);
  procedure testEnumeration(expectedCount: integer);
end;
const MISSING = 'MISSING';

type TTestThread = class(TThread)
  id: string;
  map: TMutableMapStringString;
  constructor Create(aid: string; amap: TMutableMapStringString);
  procedure Execute; override;
end;


{$PUSH}
{$RangeChecks OFF}
{$OverflowChecks OFF}
class function THAMTTestTypeInfo.hash(const s: string): THAMTHash;
var
  p, last: PByte;
begin
  if s = '' then exit(1);
  p := pbyte(pointer(s));
  last := p + length(s);
  result := 0;

  //testing cases
  if p^ = ord('_') then begin
    inc(p);
    while p < last do begin
      result := (result shl BITS_PER_LEVEL) or THAMTHash((p^ - ord('0')) * 10 + (((p+1)^ - ord('0') )));
      inc(p, 3);
    end;
    exit;
  end;
  if p^ = ord('$') then exit(StrToInt(s));
  case s of
  'test', 'collision+1', 'collision+2', 'collision+3': exit(hash('collision'));
  end;

  //actual hash
  while p < last do begin
    result := result + p^;
    result := result + (result shl 10);
    result := result xor (result shr 6);
    inc(p);
  end;


  result := result + (result shl 3);
  result := result xor (result shr 11);
  result := result + (result shl 15);
end;
{$POP}


procedure TMutableMap_Test.testInsert(const k, v: string; override: boolean);
var c: integer;
begin
  c := count;
  test(insert(k, v) xor override, 'insert failed (override marker?)');
  test(contains(k), 'insert failed: ' + k);
  test(get(k, 'xx'), v);
  if not override then inc(c);
  test(count, c);
  test((c = 0) = isEmpty);
end;

procedure TMutableMap_Test.testGet(const k, v: string);
begin
  test(get(k, MISSING), v, 'get ' + k);
end;

procedure TMutableMap_Test.testRemove(const k: string; notthere: boolean);
var c: integer;
begin
  c := count;
  if notthere then
    test(THAMTNode.removeIfThere(@froot, PPair(@k)^) <> notthere, 'remove failed: ' + k)
   else remove(k);
//    test( <> notthere, 'remove failed: ' + k);
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
  test(result[k], v);
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

constructor TTestThread.Create(aid: string; amap: TMutableMapStringString);
begin
  id := aid;
  map := amap;
  inherited Create(false);
end;

var runningThreads: integer;
procedure TTestThread.Execute;
var
  i: Integer;
begin
  for i := 1 to 50000 do
    map.insert(id + IntToStr(i), IntToStr(i) + id);
  for i := 1 to 50000 do
    test(map.get(id + IntToStr(i)), IntToStr(i) + id);
  for i := 1 to 50000 do
    test(map.get(IntToStr(i)), 'init' + IntToStr(i));
  for i := 1 to 50000 do
    map.remove(IntToStr(i));
  for i := 1 to 50000 do
    test(map.get(id + IntToStr(i)), IntToStr(i) + id);
  for i := 1 to 50000 do
    test(map.contains(IntToStr(i)) = false);

  map.free;
  InterLockedDecrement(runningThreads);
end;

procedure setTestsMutable;
var stringSet: TMutableSetString;
    p: TMutableSetString.PItem;
begin
  stringSet := TMutableSetString.create;
  stringSet.Insert('hello');
  stringSet.insert('foo');

  test(stringSet['hello']);
  test(stringSet.contains('foo'));
  test(not stringSet.contains('abc'));

  //enumerate all
  for p in stringSet do
    test((p^ = 'hello') or (p^ = 'foo'));

  stringSet.free;
end;

procedure setTestsImmutable;
var set1, set2, set3: TImmutableSetString;
  p: TImmutableSetString.PItem;
begin
  set1 := TImmutableSetString.create;
  set2 := set1.Insert('hello');
  set3 := set2.insert('foo');

  test(not set1.contains('hello'));
  test(not set1['foo']);

  test(set2.contains('hello'));
  test(not set2['foo']);

  test(set3.contains('hello'));
  test(set3['foo']);

  //enumerate all
  for p in set3 do
    test((p^ = 'hello') or (p^ = 'foo'));

  set1.free;
  set2.free;
  set3.free;
end;

var
  hamt, hamt2: TMutableMap_Test;
  imap, imap2, imap3, imap4: TImmutableMapStringString;
  mss: TMutableMapStringString;
  i: Integer;
  threads: array[1..8] of TThread;
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
  hamt['abc'] := 'def';
  hamt.testGet('abc', 'def');
  hamt.free;

  Assert(THAMTTestTypeInfo.hash('test') = THAMTTestTypeInfo.hash('collision'));
  Assert(THAMTTestTypeInfo.hash('_00') = 0);
  Assert(THAMTTestTypeInfo.hash('_01_02') = 2 or 32);
  Assert(THAMTTestTypeInfo.hash('$123') = $123);

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

  //test some keys
  hamt := TMutableMap_Test.create;
  hamt.testInsert('$0', '0x0');
  hamt.testInsert('$1', '0x1');
  hamt.testInsert('$2', '0x2');
  hamt.testInsert('$3', '0x3');
  hamt.testInsert('$FFFFFFFF', '0xFFFFFFFF');
  hamt.testInsert('$FFFFFFFE', '0xFFFFFFFE');
  hamt.testInsert('$EFFFFFFF', '0xEFFFFFFF');
  hamt.testInsert('$EFFFFFFE', '0xEFFFFFFE');
  hamt.testInsert('$7FFFFFFE', '0x7FFFFFFE');
  hamt.testInsert('$7FFFFFFF', '0x7FFFFFFF');
  hamt.testInsert('$FFFFFFF0', '0xFFFFFFF0');
  hamt.testInsert('$FFFFFFF1', '0xFFFFFFF1');
  hamt.testInsert('$FFFFFFF2', '0xFFFFFFF2');
  hamt.testInsert('$FFFFFFF3', '0xFFFFFFF3');
  hamt.testInsert('$eFFFFFF0', '0xeFFFFFF0');
  hamt.testInsert('$eFFFFFF1', '0xeFFFFFF1');
  hamt.testInsert('$eFFFFFF2', '0xeFFFFFF2');
  hamt.testInsert('$eFFFFFF3', '0xeFFFFFF3');
  hamt.testInsert('$eFFFFFF7', '0xeFFFFFF7');
  hamt.testInsert('$eFFFFFF8', '0xeFFFFFF8');
  hamt.testEnumeration(20);
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

  setTestsMutable;
  setTestsImmutable;

  mss := TMutableMapStringString.Create();
  for i := 1 to 50000 do
    mss.insert(IntToStr(i), 'init' + IntToStr(i));
  runningThreads := 8;
  for i := 1 to 8 do
    threads[i] := TTestThread.Create('_'+IntToStr(i)+'_', mss.clone);
  mss.free;

  while runningThreads > 0 do sleep(100);
  for i := 1 to 8 do threads[i].free;

  writeln('ok');
end.

