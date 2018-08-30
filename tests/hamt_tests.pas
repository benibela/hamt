program hamt_tests;

{$mode objfpc}{$H+}{$ModeSwitch typehelpers}

uses
  {$IFDEF UNIX}{$IFDEF UseCThreads}
  cthreads,
  {$ENDIF}{$ENDIF}
  Classes, bbhamt, commontestutils
  { you can add units after this };

type THAMT_Test = object(THAMT)
  procedure testInsert(const k, v: string; override: boolean = false);
  procedure testGet(const k, v: string);
end;

var
  hamt: THAMT_Test;

procedure THAMT_Test.testInsert(const k, v: string; override: boolean);
begin
  test(hamt.insert(k, v) xor override);
  test(hamt.contains(k));
  test(hamt.get(k, 'xx'), v);
end;

procedure THAMT_Test.testGet(const k, v: string);
begin
  test(hamt.get(k, 'xx'), v);
end;

begin
  hamt.init;
  hamt.testInsert('hello', 'world');

  test(not hamt.contains('xyz'));

  hamt.testInsert('foo', 'bar');

  hamt.get('hello', 'world');
  hamt.testGet('foo', 'bar');

  hamt.testInsert('hello', 'override', true);

  hamt.testGet('foo', 'bar');

  hamt.release;



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
end.

