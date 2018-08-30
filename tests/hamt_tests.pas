program hamt_tests;

{$mode objfpc}{$H+}{$ModeSwitch typehelpers}

uses
  {$IFDEF UNIX}{$IFDEF UseCThreads}
  cthreads,
  {$ENDIF}{$ENDIF}
  Classes, bbhamt, commontestutils, sysutils, bbutils, strutils
  { you can add units after this };

type THAMT_Test = object(THAMTStringString) //object(specialize THAMT<string, string, THAMTTypeInfo>)
  procedure testInsert(const k, v: string; override: boolean = false);
  procedure testGet(const k, v: string);
end;

const MISSING = 'MISSING';



procedure THAMT_Test.testInsert(const k, v: string; override: boolean);
begin
  test(insert(k, v) xor override, 'insert failed (override marker?)');
  test(contains(k), 'insert failed: ' + k);
  test(get(k, 'xx'), v);
end;

procedure THAMT_Test.testGet(const k, v: string);
begin
  test(get(k, MISSING), v, 'get ' + k);
end;

var
  hamt, hamt2: THAMT_Test;
begin
  hamt.init;
  hamt.testInsert('hello', 'world');
  test(not hamt.contains('xyz'));
  hamt.testInsert('foo', 'bar');

  hamt.testGet('hello', 'world');
  hamt.testGet('foo', 'bar');

  hamt.testInsert('hello', 'override', true);

  hamt.testGet('foo', 'bar');

  hamt.release;


  //test collisions
  hamt.init;
  hamt.testInsert('test', 'A');
  hamt.testInsert('collision', 'B');
  hamt.testInsert('collision+1', 'C');
  hamt.testInsert('collision+2', 'D');

  hamt.testGet('test', 'A');
  hamt.testGet('collision', 'B');
  hamt.testGet('collision+1', 'C');
  hamt.testGet('collision+2', 'D');
  hamt.release;


  hamt.init;
  hamt.testInsert('_00_01_02', 'x1');
  hamt.testInsert('_00_01_02', 'x2', true);
  hamt.testInsert('_00_02_03', 'x3');

  hamt.testGet('_00_01_02', 'x2');
  hamt.testGet('_00_02_03', 'x3');
  hamt.release;

  hamt.init;
  hamt.testInsert('_00_01_02', 'x1');
  hamt.testInsert('_00_01-02', 'x1b');
  hamt.testInsert('_00_01-03', 'y');
  hamt.testGet('_00_01_02', 'x1');
  hamt.testGet('_00_01-02', 'x1b');
  hamt.testGet('_00_01-03', 'y');
  hamt.release;

  //test prefix collisions
  hamt.init;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02_01_00', 'x2', true);
  hamt.testInsert('_03_02_00', 'x3');

  hamt.testGet('_02_01_00', 'x2');
  hamt.testGet('_03_02_00', 'x3');
  hamt.release;

  hamt.init;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02x01_00', 'x2');
  hamt.testInsert('_03_02_00', 'x3');

  hamt.testGet('_02_01_00', 'x1');
  hamt.testGet('_02x01_00', 'x2');
  hamt.testGet('_03_02_00', 'x3');
  hamt.release;


  //test snapshots
  hamt.init;
  hamt.testInsert('hello', 'world');
  hamt.testInsert('foo', 'bar');
  hamt2 := THAMT_Test(hamt.snapshot);
  hamt.testInsert('hello', 'override', true);

  hamt.testGet('hello', 'override');
  hamt.testGet('foo', 'bar');

  hamt2.testGet('hello', 'world');
  hamt2.testGet('foo', 'bar');
  hamt.release;
  hamt2.release;


  hamt.init;
  hamt.testInsert('hello', 'world');
  hamt.testInsert('foo', 'bar');
  hamt2 := THAMT_Test(hamt.snapshot);
  hamt.testInsert('new', 'N');

  hamt.testGet('hello', 'world');
  hamt.testGet('foo', 'bar');
  hamt.testGet('new', 'N');

  hamt2.testGet('hello', 'world');
  hamt2.testGet('foo', 'bar');
  hamt2.testGet('new', MISSING);
  hamt.release;
  hamt2.release;


  hamt.init;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02_01_00', 'x2', true);
  hamt2 := THAMT_Test(hamt.snapshot);
  hamt.testInsert('_03_02_00', 'x3');

  hamt.testGet('_02_01_00', 'x2');
  hamt.testGet('_03_02_00', 'x3');
  hamt2.testGet('_02_01_00', 'x2');
  hamt2.testGet('_03_02_00', MISSING);

  hamt.testInsert('_03_03_00', 'x4');
  hamt2.release;
  hamt2 := THAMT_Test(hamt.snapshot);
  hamt.testInsert('_03_03_00', 'x5', true);

  hamt.testGet('_02_01_00', 'x2');
  hamt.testGet('_03_02_00', 'x3');
  hamt.testGet('_03_03_00', 'x5');
  hamt2.testGet('_02_01_00', 'x2');
  hamt2.testGet('_03_02_00', 'x3');
  hamt2.testGet('_03_03_00', 'x4');

  hamt.release;
  hamt2.release;



  hamt.init;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02-01_00', 'x2');
  hamt2 := THAMT_Test(hamt.snapshot);
  hamt.testInsert('_03_02_00', 'x3');

  hamt.testGet('_02_01_00', 'x1');
  hamt.testGet('_02-01_00', 'x2');
  hamt.testGet('_03_02_00', 'x3');
  hamt2.testGet('_02_01_00', 'x1');
  hamt2.testGet('_02-01_00', 'x2');
  hamt2.testGet('_03_02_00', MISSING);
  hamt.release;
  hamt2.release;



  hamt.init;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02-01_00', 'x2');
  hamt2 := THAMT_Test(hamt.snapshot);
  hamt.testInsert('_02x01_00', 'x3');

  hamt.testGet('_02_01_00', 'x1');
  hamt.testGet('_02-01_00', 'x2');
  hamt.testGet('_02x01_00', 'x3');
  hamt2.testGet('_02_01_00', 'x1');
  hamt2.testGet('_02-01_00', 'x2');
  hamt2.testGet('_02x01_00', MISSING);
  hamt.release;
  hamt2.release;


  hamt.init;
  hamt.testInsert('_02_01_00', 'x1');
  hamt.testInsert('_02-01_00', 'x2');
  hamt2 := THAMT_Test(hamt.snapshot);
  hamt.testInsert('_02-01_00', 'x3', true);

  hamt.testGet('_02_01_00', 'x1');
  hamt.testGet('_02-01_00', 'x3');
  hamt2.testGet('_02_01_00', 'x1');
  hamt2.testGet('_02-01_00', 'x2');
  hamt.release;
  hamt2.release;


end.

