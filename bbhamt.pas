{
Copyright (C) 2018         Benito van der Zander (BeniBela)
                           benito@benibela.de
                           www.benibela.de

This file is distributed under under the same license as Lazarus and the LCL itself:

This file is distributed under the Library GNU General Public License
with the following modification:

As a special exception, the copyright holders of this library give you
permission to link this library with independent modules to produce an
executable, regardless of the license terms of these independent modules,
and to copy and distribute the resulting executable under terms of your choice,
provided that you also meet, for each linked independent module, the terms
and conditions of the license of that module. An independent module is a
module which is not derived from or based on this library. If you modify this
library, you may extend this exception to your version of the library, but
you are not obligated to do so. If you do not wish to do so, delete this
exception statement from your version.

}
{**
@abstract(Mutable and immutable maps as hash array mapped trie (HAMT))

Public generic classes:

* TReadOnlyMap

* TMutableMap

* TImmutableMap

Public specialized classes:

* TMutableMapStringString

* TMutableMapStringObject

* TImmutableMapStringString

* TImmutableMapStringObject

}
unit bbhamt;

{$mode objfpc}{$H+}{$ModeSwitch autoderef}{$ModeSwitch advancedrecords}

interface

uses sysutils;
type
  ppointer = ^pointer;

  THAMTHash = Cardinal;
  THAMTBitmap = record
    function countBeforeIndex(index: THAMTHash): DWord; inline;
    case boolean of
      false: (bits: bitpacked array[0..31] of boolean);
      true: (all: THAMTHash);
  end;
  { 4 cases                                           bitmapIsSinglePointer          bitmapIsValue        pointer tag
     - next hamt node (pointer)                             true                        false                 0
     - list (pointer to multiple keys + values)             true                        false                 1
     - empty                                                false                       false                 -
     - one pair (key + value)                               false                       true                  -
  }
  THAMTTaggedPointer = packed record
    raw: pointer;
    function unpack(out isArray: boolean): pointer; inline;
    procedure setToArray(p: pointer); inline;
  end;
const
  BITS_PER_LEVEL = 5;
  LEVEL_HIGH = ( sizeof(THAMTHash) * 8 ) div BITS_PER_LEVEL;
type
  EHAMTKeyNotFound = class(Exception);
  //** @abstract(Low-level HAMT from which the maps are built)
  //** Each node has a reference counter and stores up to 32 pointers or (key,value) pairs. @br
  //** The HAMT node is either mutable (if reference counter is 1) or immutable with copy-on-write (if reference counter is >= 2) like strings. @br
  //** Using the nodes directly would be more efficient than using the map classes, since you have one less memory access without the class instance.
  generic THAMTNode<TKey, TValue, TInfo> = packed object
    type
    PKey = ^TKey;
    PValue = ^TValue;
    TPair = packed record
      key: TKey;
      value: TValue;
      procedure addRef(); inline;
      procedure release(); inline;
    end;
    PPair = ^TPair;
    {Invariants:
    - THAMTArray is not empty, count > 0
    - All keys in one THAMTArray have the same hash
    }
    PHAMTArray = ^THAMTArray;
    THAMTArray = packed object
      refCount: Integer;
      count: integer;
      firstPair: array[0..0] of TPair;
      class function size(aCount: integer): SizeInt; static; inline;
      class function allocate(acount: integer): PHAMTArray; static;
      class procedure decrementRefCount(a: PHAMTArray); static;
      procedure incrementChildrenRefCount;
      function indexOf(const key: TKey): integer;
      function find(const key: TKey): PPair;
      function get(i: Integer): PPair; inline;
      property pairs[i: Integer]: PPair read get; default;
      //todo: this can easily be extended to a multi map, by having multiple pairs with the same key
    end;
    PHAMTNode = ^THAMTNode;
    PPHAMTNode = ^PHAMTNode;
    THAMTEnumerator = object
    protected
      level: integer;
      stack: array[0..LEVEL_HIGH] of PHAMTNode;
      offsets: array[0..LEVEL_HIGH] of integer;
      fcurrent, pairLast: PPair;
      function pushNode(node: PHAMTNode): boolean;
    public
      procedure initialize(node: PHAMTNode);
      property current: PPair read fcurrent;
      function MoveNext: boolean;
    end;
    TKeySizeEquivalent = packed array[1..sizeof(TKey)] of byte;
    TValueSizeEquivalent = packed array[1..sizeof(TValue)] of byte;
    TPairSizeEquivalent = packed array[1..sizeof(TKey)+sizeof(TValue)] of byte;
//    PKeySizeEquivalent = ^TKeySizeEquivalent;
//    PValueSizeEquivalent = ^TValueSizeEquivalent;
    PPairSizeEquivalent = ^TPairSizeEquivalent;

  protected
    refCount: Integer;
    pointerCount, pairCount: word;
    bitmapIsSinglePointer: THAMTBitmap;
    bitmapIsValue: THAMTBitmap;
    pointers: array[0..63] of THAMTTaggedPointer;
    class procedure raiseMissingKey(const key: TKey); static;
    function getPointerOffset(index: THAMTHash): DWord; inline;
    //function getPointer(index: THAMTHash): pointer; inline;
    function getPairOffset(index: THAMTHash): DWord; inline;
    function getPairFromOffset(offset: DWord): PPair; inline;
    function getPairAddr(index: THAMTHash): PPair; inline;
    function getNodeFromOffset(offset: DWord): PHAMTNode; inline;
    class procedure hashShift(var hash: THAMTHash; out index: THAMTHash); static; inline;
    class function size(apointerCount, apairCount: integer): SizeInt; static; inline;
    procedure incrementChildrenRefCount;
    class procedure assignKeyRef(var target: TKey; const source: TKey); static; inline;
    class procedure assignValueRef(var target: TValue; const source: TValue); static; inline;
  public
    class procedure decrementRefCount(node: PHAMTNode); static;
    class function allocate(apointerCount, apairCount: integer): PHAMTNode; static;
  public
    //trigger copy-on-write so the node becomes mutable
    class function uniqueNode(ppnode: PPHAMTNode): PHAMTNode;
    {
    //insert override allowed = true
      result = true                  inserted, no override
      result = false                 inserted, override
    //insert override forbidden = false
      result = true                  inserted, (no override)
      result = false                 not inserted
    }
    class function insert(ppnode: PPHAMTNode; const key: TKey; const value: TValue; allowOverride: boolean): Boolean; static;
    class function removeIfThere(ppnode: PPHAMTNode; const key: TKey): Boolean; static;
    function find(const key:TKey): PPair;
    function contains(const key:TKey):boolean;
    function get(const key: TKey; const def: TValue): TValue;
    function get(const key: TKey): TValue;
  end;


  //** @abstract(Default hash function and reference counting for strings/objects)
  //** The HAMT requires a hash function (hash) and equality test (equal) to compare keys. @br
  //** Memory management requires methods for reference counting (addRef, release) . Reference counting is mandatory as the HAMT might make arbitrary many copies of everything. @br
  //** You can derive an object of THAMTTypeInfo to change some methods, e.g., the hashing.
  THAMTTypeInfo = object
    class function hash(const s: string): THAMTHash;
    class function equal(const s, t: string): boolean;

    class procedure addRef(var k: string); inline;
    class procedure release(var k: string); inline;
    class function toString(const k: string): string; inline;

    class procedure addRef(var k: IUnknown); inline;
    class procedure release(var k: IUnknown); inline;

    //no reference counting for objects
    class procedure addRef(var {%H-}k: TObject); inline;
    class procedure release(var {%H-}k: TObject); inline;
    class function toString(const k: TObject): string; inline;
  end;

  //** @abstract(Generic read-only map)
  //**
  //** The data in this map can be read, but there are no methods to modify it.
  generic TReadOnlyMap<TKey, TValue, TInfo> = class
    type THAMTNode = specialize THAMTNode<TKey, TValue, TInfo>;
         PHAMTNode = ^THAMTNode;
         PPair = THAMTNode.PPair;
    protected
      fcount: SizeInt;
      froot: PHAMTNode;
    public
      //** Returns if the map is empty
      function isEmpty: boolean; inline;
      //** Returns if the map contains a certain key
      function contains(const key:TKey): boolean; inline;
      //** Returns the value for a certain key, or default value def if the map does not contain the key
      function get(const key: TKey; const def: TValue): TValue; inline;
      //** Returns the value for a certain key, or raises an exception if the map does not contain the key
      function get(const key: TKey): TValue; inline;
      //** Enumerates all (key, value) pairs in an unspecified order @br
      //** Example: @longcode(#
      //** var p: TReadOnlyMap.PPair;
      //** for p in map do
      //**    .. p^.key .. p^.value ..
      //** #)
      //**
      //**The pointer let's you modify the pair, but you must not modify it.
      function getEnumerator: THAMTNode.THAMTEnumerator;
      destructor Destroy; override;
      //** Number of (key, value) pairs in the map
      property count: SizeInt read fcount;
      //** Default parameter, so you can read elements with @code(map[key])
      property items[key: TKey]: TValue read get; default;
  end;

  {** @abstract(Generic mutable map)

  Data in this map can be read (see ancestor TReadOnlyMap) and modified.

  Example:

  @longcode(#
    type TMutableMapStringString = specialize TMutableMap<string, string, THAMTTypeInfo>;
    var map: TMutableMapStringString;
        p: TMutableMapStringString.PPair;
    begin
      map := TMutableMapStringString.create;
      map.Insert('hello', 'world');
      map.insert('foo', 'bar');
      map['abc'] := 'def';

      writeln(map['hello']); // world
      writeln(map.get('foo')); // bar
      writeln(map.get('abc', 'default')); // def

      //enumerate all
      for p in map do
        writeln(p^.key, ': ', p^.value);

      map.free;
    end.
  #)
  }
  generic TMutableMap<TKey, TValue, TInfo> = class(specialize TReadOnlyMap<TKey, TValue, TInfo>)
  private
    procedure insertItem(const key: TKey; const value: TValue);
  public
    //** Creates an empty map
    constructor Create;
    //** Creates a map equal to other. No data is copied, till either map is modified (copy-on-write).
    constructor Create(other: specialize TReadOnlyMap<TKey, TValue, TInfo>);
    //** Inserts a (key, value) pair, if the map does not contain key or allowOverride is true.
    //** @returns If the map did not contain key.
    function insert(const key: TKey; const value: TValue; allowOverride: boolean = true): boolean;
    //** Removes key (and the associated value), or raises an exception if the map did not contain key
    procedure remove(const key:TKey); inline;
    //** Removes everything from the map;
    procedure clear;
    //** Creates a new map equal to self. No data is copied, till either map is modified (copy-on-write).
    function clone: TMutableMap;
    //** Default parameter, so you can read or write elements with @code(map[key])
    property items[key: TKey]: TValue read get write insertItem; default;
  end;

  {** @abstract(Generic immutable map)

  Data in this map can be read (see ancestor TReadOnlyMap) and modified by creating new maps.

  Example: @longcode(#
  type TImmutableMapStringString = specialize TImmutableMap<string, string, THAMTTypeInfo>;
  var map, map2, map3: TImmutableMapStringString;
    p: TImmutableMapStringString.PPair;
  begin
    map := TImmutableMapStringString.create;
    map2 := map.Insert('hello', 'world');
    map3 := map2.insert('foo', 'bar');

    writeln(map.get('hello', 'default')); // default
    writeln(map.get('foo', 'default')); // default

    writeln(map2.get('hello')); // world
    writeln(map2.get('foo', 'default')); // default

    writeln(map3['hello']); // world
    writeln(map3['foo']); // bar

    //enumerate all
    for p in map3 do
      writeln(p^.key, ': ', p^.value);

    map.free;
    map2.free;
    map3.free;
  end.
  #)
  }
  generic TImmutableMap<TKey, TValue, TInfo> = class(specialize TReadOnlyMap<TKey, TValue, TInfo>)
  public
    //** Creates an empty map
    constructor Create;
    //** Creates a map equal to other. No data is copied, till either map is modified (copy-on-write).
    constructor Create(other: specialize TReadOnlyMap<TKey, TValue, TInfo>);
    //** Creates a new map containing key @code(key). If the map does not contain key or allowOverride is true, the value associated with the key is @code(value), otherwise the value is unchanged.
    //** @returns The new map
    function insert(const key: TKey; const value: TValue; allowOverride: boolean = true): TImmutableMap;
    //** Creates a new map without key and its associated value, or raises an exception if the map did not contain key
    //** @returns The new map
    function remove(const key:TKey): TImmutableMap; inline;
    //** Creates a new map equal to self. No data is copied, till either map is modified (copy-on-write).
    function clone: TImmutableMap;
  end;

  //** @abstract(A TMutableMap mapping string keys to string values.)
  //** The map handles reference counting and freeing of the strings.
  TMutableMapStringString = specialize TMutableMap<string, string, THAMTTypeInfo>;
  //** @abstract(A TMutableMap mapping string keys to TObject values.)
  //** The map handles reference counting and freeing of the string keys, but the objects are neither changed nor freed.
  TMutableMapStringObject = specialize TMutableMap<string, TObject, THAMTTypeInfo>;
  //** @abstract(A TImmutableMap mapping string keys to string values.)
  //** The map handles reference counting and freeing of the strings.
  TImmutableMapStringString = specialize TImmutableMap<string, string, THAMTTypeInfo>;
  //** @abstract(A TImmutableMap mapping string keys to TObject values.)
  //** The map handles reference counting and freeing of the string keys, but the objects are neither changed nor freed.
  TImmutableMapStringObject = specialize TImmutableMap<string, TObject, THAMTTypeInfo>;


  function alignedGetMem(s: PtrUInt): pointer; inline;


implementation

procedure THAMTNode.TPair.addRef();
begin
  TInfo.addRef(key);
  TInfo.addRef(value);
end;

procedure THAMTNode.TPair.release();
begin
  TInfo.release(key);
  TInfo.release(value);
end;


function THAMTNode.THAMTEnumerator.pushNode(node: PHAMTNode): boolean;
begin
  inc(level);
  stack[level] := node;
  offsets[level] := -1;
  result := node.pairCount > 0;
  if result then begin
    fcurrent := node.getPairFromOffset(0);
    pairLast := current + node.pairCount - 1;
  end;
end;

procedure THAMTNode.THAMTEnumerator.initialize(node: PHAMTNode);
begin
  level:=-1;
  if pushNode(node) then dec(fcurrent)
  else begin
    fcurrent := nil;
    pairLast := nil;
  end;
end;

function THAMTNode.THAMTEnumerator.MoveNext: boolean;
var
  node: PHAMTNode;
  p: Pointer;
  isArray: boolean;
begin
  result := current < pairLast;
  if result then begin
    inc(fcurrent);
    exit;
  end;

  while level >= 0 do begin
    node := stack[level];
    inc(offsets[level]);
    if offsets[level] < node.pointerCount then begin
      p := node.pointers[offsets[level]].unpack(isArray);
      if isArray then begin
        fcurrent := @PHAMTArray(p).firstPair[0];
        pairLast := fcurrent + PHAMTArray(p).count - 1;
        assert(fcurrent <= pairLast);
        exit(true);
      end;
      result := pushNode(PHAMTNode(p));
      if result then exit;
    end else dec(level);
  end;

  result := false;
end;


{$PUSH}
{$RangeChecks OFF}
{$OverflowChecks OFF}
class function THAMTTypeInfo.hash(const s: string): THAMTHash;
var
  p, last: PByte;
begin
  if s = '' then exit(1);
  p := pbyte(pointer(s));
  last := p + length(s);
  result := 0;

  //testing cases
  case s of
  'test', 'collision+1', 'collision+2', 'collision+3': exit(hash('collision'));
  end;
  if p^ = ord('_') then begin
    inc(p);
    while p < last do begin
      result := (result shl BITS_PER_LEVEL) or ((p^ - ord('0')) * 10 + (((p+1)^ - ord('0') )));
      inc(p, 3);
    end;
    exit;
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

class function THAMTTypeInfo.equal(const s,t:string): boolean;
begin
  result := s = t;
end;

Procedure fpc_AnsiStr_Incr_Ref (S : Pointer); [external name 'FPC_ANSISTR_INCR_REF'];
Procedure fpc_ansistr_decr_ref (Var S : Pointer); [external name 'FPC_ANSISTR_DECR_REF'];


class procedure THAMTTypeInfo.addRef(var k: string);
begin
  fpc_ansistr_incr_ref(pointer(k));
end;

class procedure THAMTTypeInfo.release(var k: string);
begin
  fpc_ansistr_decr_ref(pointer(k));
end;


class function THAMTTypeInfo.toString(const k: string): string;
begin
  result := k
end;

class procedure THAMTTypeInfo.addRef(var k: IUnknown);
begin
  k._AddRef;
end;

class procedure THAMTTypeInfo.release(var k: IUnknown);
begin
  k._Release;
end;

class procedure THAMTTypeInfo.addRef(var k: TObject);
begin
  //empty
end;

class procedure THAMTTypeInfo.release(var k: TObject);
begin
  //empty
end;


class function THAMTTypeInfo.toString(const k: TObject): string;
begin
  result := k.ToString;
end;

class procedure THAMTNode.hashShift(var hash: THAMTHash; out index: THAMTHash); inline;
begin
  index := hash and %11111;
  hash := hash shr BITS_PER_LEVEL;
end;

function THAMTBitmap.countBeforeIndex(index: THAMTHash): DWord; inline;
var
  mask: THAMTHash;
begin
  mask := (THAMTHash(1) shl index) - 1;
  result := PopCnt(all and mask);
end;



class function THAMTNode.THAMTArray.size(aCount: integer): SizeInt;
begin
  result := 2*sizeof(integer) + sizeof(TPair) * acount
end;

class function THAMTNode.THAMTArray.allocate(acount: integer): PHAMTArray;
begin
  result := alignedGetMem(size(acount)) ;
  result.refCount := 1;
  result.count := acount;
end;

class procedure THAMTNode.THAMTArray.decrementRefCount(a: PHAMTArray);
var
  i: Integer;
  p: PPair;
begin
  with a^ do begin
    p := @firstPair[0]; //use p[i] rather than pairs[i], because pairs[i] is not inlined
    if InterLockedDecrement(refCount) = 0 then begin
      for i := 0 to count - 1 do
        p[i].release();
      Freemem(a);
    end;
  end;
end;

procedure THAMTNode.THAMTArray.incrementChildrenRefCount;
var
  i: Integer;
  p: PPair;
begin
  p := @firstPair[0];
  for i := 0 to count - 1 do
    p[i].addRef();
end;

function THAMTNode.THAMTArray.indexOf(const key: TKey): integer;
var
  i: Integer;
  p: PPair;
begin
  p := @firstPair[0];
  for i := 0 to count - 1 do
    if TInfo.equal(p[i].key, key) then begin
      exit(i);
    end;
  result := -1;
end;

function THAMTNode.THAMTArray.find(const key: TKey): PPair;
var
  index: Integer;
begin
  index := indexOf(key);
  if index >= 0 then begin
    result :=  @firstPair[0];
    inc(result, index);
  end else result := nil;
end;

function THAMTNode.THAMTArray.get(i: Integer): PPair;
begin
  result := @firstPair[0];
  inc(result, i);
end;

{$push}
{$WARN 4055 off : Conversion between ordinals and pointers is not portable}
function alignedGetMem(s: PtrUInt): pointer; inline;
begin
  result := GetMem(s);
  while PtrUInt(result) and 1 = 1 do begin
    Freemem(result);
    result := GetMem(s);
  end;
end;



function THAMTTaggedPointer.unpack(out isArray: boolean): pointer; inline;
var
  tag: PtrUInt;
begin
  tag := PtrUInt(raw) and 1;
  isArray := tag <> 0;
  result := pointer(PtrUInt(raw) and not tag);
end;

procedure THAMTTaggedPointer.setToArray(p: pointer);
begin
  raw := pointer(PtrUInt(p) or 1);
end;
{$pop}

class procedure THAMTNode.raiseMissingKey(const key: TKey);
begin
  raise EHAMTKeyNotFound.Create('Key not found: '+TInfo.toString(key));
end;

function THAMTNode.getPointerOffset(index: THAMTHash): DWord;
begin
  result := bitmapIsSinglePointer.countBeforeIndex(index);
end;

function THAMTNode.getPairOffset(index: THAMTHash): DWord;
begin
  result := bitmapIsValue.countBeforeIndex(index);
end;

function THAMTNode.getPairFromOffset(offset: DWord): PPair;
begin
  result := PPair(@pointers[pointerCount]) + offset;
end;

function THAMTNode.getPairAddr(index: THAMTHash): PPair;
begin
  result := getPairFromOffset(getPairOffset(index));
end;

function THAMTNode.getNodeFromOffset(offset: DWord): PHAMTNode;
begin
  result := PHAMTNode(pointers[offset].raw);
end;


procedure THAMTNode.incrementChildrenRefCount;
var
  isArray: boolean;
  pointerRaw: Pointer;
  pairs: PPair;
  i: Integer;
begin
  for i := 0 to pointerCount - 1 do begin
    pointerRaw := pointers[i].unpack(isArray);
    if isArray then InterLockedIncrement(PHAMTArray(pointerRaw).refCount)
    else InterLockedIncrement(PHAMTNode(pointerRaw).refCount)
  end;
  pairs := PPair(@pointers[pointerCount]);
  for i := 0 to pairCount - 1 do
    pairs[i].addRef();
end;

class procedure THAMTNode.assignKeyRef(var target: TKey; const source: TKey);
begin
  TInfo.release(target);
  TKeySizeEquivalent(target) := TKeySizeEquivalent(source);
  TInfo.addRef(target);
end;

class procedure THAMTNode.assignValueRef(var target: TValue; const source: TValue);
begin
  TInfo.release(target);
  TValueSizeEquivalent(target) := TValueSizeEquivalent(source);
  TInfo.addRef(target);
end;

class procedure THAMTNode.decrementRefCount(node: PHAMTNode);
var
  isArray: boolean;
  pointerRaw: Pointer;
  pairs: PPair;
  i: Integer;
begin
  with node^ do begin
    if InterLockedDecrement(refCount) = 0 then begin
      for i := 0 to pointerCount - 1 do begin
        pointerRaw := pointers[i].unpack(isArray);
        if isArray then THAMTArray.decrementRefCount(PHAMTArray(pointerRaw))
        else decrementRefCount(PHAMTNode(pointerRaw))
      end;
      pairs := PPair(@pointers[pointerCount]);
      for i := 0 to pairCount - 1 do
        pairs[i].release();
      Freemem(node);
    end;
  end;
end;

class function THAMTNode.size(apointerCount, apairCount: integer): SizeInt;
begin
  size := SizeOf(THAMTNode.refCount) + SizeOf(THAMTNode.pointerCount) + SizeOf(THAMTNode.pairCount) + SizeOf(THAMTNode.bitmapIsSinglePointer) + SizeOf(THAMTNode.bitmapIsValue)
                   + SizeOf(Pointer) * apointerCount
                   + SizeOf(TPair) * apairCount;
end;

class function THAMTNode.allocate(apointerCount, apairCount: integer): PHAMTNode;
var
  s: SizeInt;
begin
  s := size(apointerCount, apairCount);
  result := alignedGetMem(s);
  FillChar(result^, s, 0);
  result^.refCount := 1;
  result^.pointerCount := apointerCount;
  result^.pairCount := apairCount;
end;

class function THAMTNode.uniqueNode(ppnode: PPHAMTNode): PHAMTNode;
var
  s: SizeInt;
begin
  result := ppnode^;
  if result.refCount > 1 then begin
    {          [ refcount = 2 ]                 =>          [ refcount = 1 ]            [ refcount = 1 ]
     [ refcount = 1 ]        [ refcount = 1 ]                  [ refcount = 2 ]        [ refcount = 2 ]
     }
    result.incrementChildrenRefCount;

    s := size(result.pointerCount, result.pairCount);
    ppnode^ := alignedGetMem(s); //new node
    move(result^, ppnode^^, s);
    decrementRefCount(result); //result is still the old node
    ppnode^^.refCount := 1;
    result := ppnode^;
  end;
end;


class function THAMTNode.insert(ppnode: PPHAMTNode; const key: TKey; const value: TValue; allowOverride: boolean): Boolean;

var pairIndex: Integer;
  function cloneArray(hamtArrayx: pointer): pointer;
  var HAMTArray: PHAMTArray;
    pair: PPair;
  begin
    HAMTArray := PHAMTArray(hamtArrayx);
    if pairIndex < 0 then begin
      result := THAMTArray.allocate(hamtArray.count + 1);
      pairIndex := hamtArray.count;
      pair := PHAMTArray(result)^[pairIndex];
      TKeySizeEquivalent(pair.key) := TKeySizeEquivalent(key);
      TInfo.addRef(pair.key);
      TValueSizeEquivalent(pair.value) := default(TValueSizeEquivalent);
    end else
      result := THAMTArray.allocate(hamtArray.count);
    move(hamtArray^[0]^, PHAMTArray(result)^[0]^, hamtArray.count * sizeof(TPair));
    if hamtArray.refCount > 1 then begin
      hamtArray.incrementChildrenRefCount();
      THAMTArray.decrementRefCount(hamtArray);
    end else
      Freemem(hamtArray);
  end;

var
  i: Integer;
  h, index, h2: THAMTHash;
  offset: DWord;
  procedure movePairsDown(pairsIsArray: boolean; pairs: pointer);
  var node : PHAMTNode;
      dataOffset: integer;
      pair: PPair;
      index2: THAMTHash;
  begin
    ppnode := @ppnode^.pointers[offset].raw;
    while i <= LEVEL_HIGH do begin
      hashShift(h, index);
      hashShift(h2, index2);
      if index = index2 then begin
        //go to next level
        node := THAMTNode.allocate(1, 0);
        node.bitmapIsSinglePointer.all := THAMTHash(1) shl index;
        node.bitmapIsValue.all := 0;
        ppnode^ := node;
        ppnode := @node.pointers[0].raw;
      end else begin
        //create node of old pairs and new pair
        if pairsIsArray then begin
          node := THAMTNode.allocate(1, 1);
          node.bitmapIsSinglePointer.all := THAMTHash(1) shl index2;
          node.bitmapIsValue.all := THAMTHash(1) shl index;
          node.pointers[0].setToArray(pairs);
          dataOffset := 0;
        end else begin
          node := THAMTNode.allocate(0, 2);
          node.bitmapIsSinglePointer.all := 0;
          node.bitmapIsValue.all := (THAMTHash(1) shl index) or (THAMTHash(1) shl index2);
          if index < index2 then dataOffset := 0 else dataOffset := 1;
          pair := node.getPairFromOffset(1 - dataOffset);
          PPairSizeEquivalent(pair)^ := PPairSizeEquivalent(pairs)^;
          pair.addRef();
        end;
        pair := node.getPairFromOffset(dataOffset);
        assignKeyRef(pair.key, key);
        assignValueRef(pair.value, value);
        ppnode^ := node;
        exit;
      end;
    end;
    assert(false);
  end;

var
  node: PHAMTNode;
  pairOffset: DWord;
  pair: PPair;
  pointerIsArray: boolean;
  rawPointer: Pointer;
  hamtArray: PHAMTArray;
begin
  result := true;
  node := ppnode^;
  h := TInfo.hash(key);
  //writeln('insert: ', key, ' ', h);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);

    if node.bitmapIsSinglePointer.bits[index] then begin
      node := UniqueNode(ppnode);
      offset := node.getPointerOffset(index);
      rawPointer := node.pointers[offset].unpack(pointerIsArray);
      if pointerIsArray then begin
        hamtArray := PHAMTArray(rawPointer);
        h2 := TInfo.hash(hamtArray^[0].key) shr (BITS_PER_LEVEL * i + BITS_PER_LEVEL);
        if h <> h2 then begin
          //child at index is an array where the keys have a different hash
          //=> move array to a lower level on which the array hash and new key hash end up at a different index
          movePairsDown(true, hamtArray);
        end else begin
          //array and new key have same hash => insert into array
          {Cases:
          key already in array           refCount = 1         => override value
          key already in array           refCount > 1         => clone array, override value in new array
          key not in array               refCount = 1         => resize array: move values from old to new array (do not change ref counts)
          key not in array               refCount > 1         => create larger array, copy values from old to new

          }
          pairIndex := hamtArray.indexOf(key);
          if (pairIndex >= 0) then begin
            result := false;
            if not allowOverride then exit;
          end;
          if (pairIndex < 0) or (hamtArray.refCount > 1) then begin
            hamtArray := cloneArray(hamtArray);
            node.pointers[offset].setToArray(hamtArray);
          end;
          assignValueRef(hamtArray^[pairIndex].value, value);
        end;
        exit;
      end else begin
        //go to next level
        ppnode := @ppnode^.pointers[offset].raw;
        node := ppnode^;
      end;
    end else if node.bitmapIsValue.bits[index] then begin
      pairOffset := node.getPairOffset(index);
      pair := node.getPairFromOffset(pairOffset);
      if not tinfo.equal(pair.key, key) then begin
        //change pair to array pointer
        if node.refCount > 1 then node.incrementChildrenRefCount;
        offset := node.getPointerOffset(index);
        ppnode^ := allocate(node.pointerCount + 1, node.pairCount - 1);
        //    [ ..head..   ..pointerPrefix..            ..pointerSuffix..    ..pairPrefix..   old pair    ..pairSuffix.. ]
        // -> [ ..head..   ..pointerPrefix.. ..newPointer ..pointerSuffix..  ..pairPrefix..             ..pairSuffix.. ]
        move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, sizeof(THAMTBitmap) + sizeof(THAMTBitmap) + sizeof(Pointer) * offset); //head, pointer prefix
        move(node.pointers[offset], ppnode^.pointers[offset + 1], sizeof(pointer) * (node.pointerCount - offset) + sizeof(TPair) * pairOffset); //..pointerSuffix..    ..pairPrefix..
        move(node.getPairFromOffset(pairOffset + 1)^ , ppnode^.getPairFromOffset(pairOffset)^, (node.pairCount - pairOffset - 1) * sizeof(TPair) ); //..pairSuffix..
        ppnode^.bitmapIsSinglePointer.bits[index] := true;
        ppnode^.bitmapIsValue.bits[index] := False;
        if i < LEVEL_HIGH then begin
          h2 := TInfo.hash(pair.key) shr (BITS_PER_LEVEL * i + BITS_PER_LEVEL);
          if h <> h2 then begin
            movePairsDown(false, pair);
            decrementRefCount(node);
            exit;
          end;
        end;
        hamtArray := THAMTArray.allocate(2);
        PPairSizeEquivalent(hamtArray^[0])^ := PPairSizeEquivalent(pair)^;
        TKeySizeEquivalent(hamtArray^[1].key) := TKeySizeEquivalent(key);
        TValueSizeEquivalent(hamtArray^[1].value) := TValueSizeEquivalent(value);
        hamtArray^[1].addRef();
        ppnode^.pointers[offset].setToArray(hamtArray);
        decrementRefCount(node);
      end else begin
        result := false;
        if not allowOverride then exit;
        node := UniqueNode(ppnode);
        assignValueRef(node.getPairFromOffset(pairOffset).value, value);
      end;
      exit;
    end else begin
      //copy node and add key+value pair
      offset := node.getPairOffset(index);
      ppnode^ := allocate(node.pointerCount, node.pairCount + 1);
      move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, sizeof(THAMTBitmap) + sizeof(THAMTBitmap) + sizeof(Pointer) * node.pointerCount + sizeof(TPair) * offset);
      move( node.getPairFromOffset(offset)^ , ppnode^.getPairFromOffset(offset+1)^ , (node.pairCount - offset) * sizeof(TPair) );
      pair := ppnode^.getPairFromOffset(offset);
      assignKeyRef(pair.key, key);
      assignValueRef(pair.value, value);
      if node.refCount > 1 then begin
        node.incrementChildrenRefCount;
        decrementRefCount(node)
      end else Freemem(node);
      ppnode^.bitmapIsValue.bits[index] := true;
      exit;
    end;
  end;
end;

class function THAMTNode.removeIfThere(ppnode: PPHAMTNode; const key: TKey): Boolean;
var
  initialPPNode: PPHAMTNode;
  indices:  array[0..LEVEL_HIGH] of THAMTHash;
  offsets:  array[0..LEVEL_HIGH] of integer; //offsets[i] := ... getPointerOffset(indices[i])
  nodes:  array[0..LEVEL_HIGH] of PHAMTNode; //nodes[0] := ppnode^; nodes[i] := nodes[i-1].pointers[offsets[i-1]].raw

  //make sure nodes[0]..nodes[tillLevel] have ref count 0
  function uniqueAncestorNodes(tillLevel: integer): pointer;
  var
    i, offset: Integer;
  begin
    result := uniqueNode(initialPPNode);
    for i := 0 to tillLevel - 1 do begin
      offset := offsets[i];
      PHAMTNode(result).pointers[offset].raw := uniqueNode(@PHAMTNode(result).pointers[offset].raw);
      result := PHAMTNode(result).pointers[offset].raw;
    end;
  end;
  //delete nodes[level].pointers[offset[level]] and all ancestors that only have one pointer to their child
  procedure deletePointerFromNode(level: integer);
  var
    deleteOffset: Integer;
    node, newNode, parentNode: PHAMTNode;
    isArray: boolean;
    p: Pointer;
  begin
    while (level >= 0) and (nodes[level].pairCount = 0) and (nodes[level].pointerCount <= 1) do
      dec(level );

    if level < 0 then begin
      THAMTNode.decrementRefCount(initialPPNode^);
      initialPPNode^ := THAMTNode.allocate(0,0);
      exit;
    end;

    if level > 0 then begin
      parentNode := uniqueAncestorNodes(level - 1);
      node := parentNode.getNodeFromOffset(offsets[level - 1]);
    end else begin
      parentNode := nil;
      node := initialPPNode^;
    end;
    newNode := allocate(node.pointerCount - 1, node.pairCount);
    deleteOffset := offsets[level];
    //    [ ..head..   ..pointerPrefix pointer pointerSuffix..    ..pairs.. ]
    // -> [ ..head..   ..pointerPrefix pointerSuffix....  ]
    move(node.bitmapIsSinglePointer, newNode.bitmapIsSinglePointer, 2*sizeof(THAMTBitmap) + sizeof(Pointer) * deleteOffset);
    move(node.pointers[deleteOffset + 1] , newNode.pointers[deleteOffset],  (node.pointerCount - deleteOffset - 1) * sizeof(pointer) + node.pairCount * sizeof(TPair) );
    newNode.bitmapIsSinglePointer.bits[indices[level]] := False;
    if node.refCount > 1 then begin
      newNode.incrementChildrenRefCount;
      THAMTNode.decrementRefCount(node);
    end else begin
      p := node.pointers[deleteOffset].unpack(isArray);
      if not isArray then THAMTNode.decrementRefCount(p)
      else THAMTArray.decrementRefCount(p);
      Freemem(node);
    end;
    if level > 0 then
      parentNode.pointers[offsets[level - 1]].raw := newNode
    else
      initialPPNode^ := newNode;
  end;

var
  hamtArray, newHamtArray: PHAMTArray;
  pairIndex: integer;
  i: Integer;
  node: PHAMTNode;
  h, index: THAMTHash;
  offset, pairOffset: DWord;
  rawPointer: Pointer;
  pointerIsArray: boolean;
  pair: PPair;
begin
  result := true;
  initialPPNode := ppnode;
  node := ppnode^;
  h := TInfo.hash(key);
  //writeln('remove: ', key, ' ', h);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);

    if node.bitmapIsSinglePointer.bits[index] then begin
      offset := node.getPointerOffset(index);
      rawPointer := node.pointers[offset].unpack(pointerIsArray);
      nodes[i] := node;
      offsets[i] := offset;
      indices[i] := index;
      if pointerIsArray then begin
        //remove from array
        hamtArray := PHAMTArray(rawPointer);
        pairIndex := hamtArray.indexOf(key);
        if pairIndex < 0 then
          exit(false);
        if hamtArray.count = 1 then begin
          deletePointerFromNode(i);
          exit;
        end;
        //todo: optimize special case hamtArray.count = 2 by converting array to in-node value
        node := uniqueAncestorNodes(i);
        newHamtArray := THAMTArray.allocate(hamtArray.count - 1);
        move(hamtArray^[0]^, newHamtArray^[0]^, sizeof(TPair) * pairIndex );
        move(hamtArray^[pairIndex + 1]^, newHamtArray^[pairIndex]^, sizeof(TPair) * ( hamtArray.count - pairIndex - 1 ) );
        if hamtArray.refCount > 1 then begin
          newHamtArray.incrementChildrenRefCount;
          THAMTArray.decrementRefCount(hamtArray);
        end else begin
          hamtArray^[pairIndex].release();
          Freemem(hamtArray);
        end;
        node.pointers[offset].setToArray(newHamtArray);
        exit;
      end else begin
        //go to next level
        ppnode := @ppnode^.pointers[offset].raw;
        node := ppnode^;
      end;
    end else if node.bitmapIsValue.bits[index] then begin
      pairOffset := node.getPairOffset(index);
      pair := node.getPairFromOffset(pairOffset);
      if not TInfo.equal(pair.key, key) then begin
        //nothing to remove
        result := false;
      end else begin
        if (node.pointerCount = 0) and (node.pairCount = 1) then begin
          deletePointerFromNode(i - 1);
          exit;
        end;
        if i > 0 then
          ppnode := @PHAMTNode(uniqueAncestorNodes(i - 1)).pointers[offset].raw;
        //remove pair
        //
        ppnode^ := allocate(node.pointerCount, node.pairCount - 1);
        //    [ ..head..   ..pointers..    ..pairPrefix..   old pair    ..pairSuffix.. ]
        // -> [ ..head..   ..pointers....  ..pairPrefix..               ..pairSuffix.. ]
        move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, 2*sizeof(THAMTBitmap) + sizeof(Pointer) * node.pointerCount + sizeof(TPair) * pairOffset);
        move(node.getPairFromOffset(pairOffset + 1)^ , ppnode^.getPairFromOffset(pairOffset)^, (node.pairCount - pairOffset - 1) * sizeof(TPair) ); //..pairSuffix..
        if node.refCount > 1 then begin
          ppnode^.incrementChildrenRefCount;
          THAMTNode.decrementRefCount(node);
        end else begin
          node^.getPairFromOffset(pairOffset).release();
          Freemem(node);
        end;
        ppnode^.bitmapIsValue.bits[index] := False;
      end;
      exit;
    end else begin
      //nothing to remove
      exit(false);
    end;
  end;
end;

function THAMTNode.find(const key: TKey): PPair;
var
  node: PHAMTNode;
  i: Integer;
  h, index: THAMTHash;
  rawPointer: Pointer;
  pointerIsArray: boolean;
begin
  node := @self;
  h := TInfo.hash(key);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);
    if node.bitmapIsSinglePointer.bits[index] then begin
      rawPointer := node.pointers[node.getPointerOffset(index)].unpack(pointerIsArray);
      if pointerIsArray then
        exit(PHAMTArray(rawPointer).find(key))
       else
        node := PHAMTNode(rawPointer)
    end else if node.bitmapIsValue.bits[index] then begin
      result := node.getPairAddr(index);
      if not TInfo.equal(result.key, key) then result := nil;
      exit;
    end else
      exit(nil);
  end;
  result := nil;
end;

function THAMTNode.contains(const key: TKey): boolean;
begin
  result := find(key) <> nil;
end;

function THAMTNode.get(const key: TKey; const def: TValue): TValue;
var
  pair: PPair;
begin
  pair := find(key);
  if pair = nil then result := def
  else result := pair.value;
end;

function THAMTNode.get(const key: TKey): TValue;
var
  pair: PPair;
begin
  pair := find(key);
  if pair = nil then raiseMissingKey(key);
  result := pair.value;
end;

function TReadOnlyMap.isEmpty: boolean;
begin
  result := count = 0;
end;

function TReadOnlyMap.contains(const key: TKey): boolean;
begin
  result := froot.contains(key);
end;

function TReadOnlyMap.get(const key: TKey; const def: TValue): TValue;
begin
  result := froot.get(key, def);
end;

function TReadOnlyMap.get(const key: TKey): TValue;
begin
  result := froot.get(key);
end;

function TReadOnlyMap.getEnumerator: THAMTNode.THAMTEnumerator;
begin
  result.initialize(froot);
end;

destructor TReadOnlyMap.Destroy;
begin
  THAMTNode.decrementRefCount(froot);
  inherited;
end;

procedure TMutableMap.insertItem(const key: TKey; const value: TValue);
begin
  insert(key, value, true);
end;

constructor TMutableMap.Create;
begin
  froot := THAMTNode.allocate(0,0);
  fcount := 0;
end;

constructor TMutableMap.Create(other: specialize TReadOnlyMap<TKey, TValue, TInfo>);
begin
  fcount := other.fcount;
  froot := other.froot;
  InterLockedIncrement(froot.refCount);
end;

function TMutableMap.insert(const key: TKey; const value: TValue; allowOverride: boolean): boolean;
begin
  result := THAMTNode.insert(@froot, key, value, allowOverride);
  if Result then Inc(fcount);
end;

procedure TMutableMap.remove(const key: TKey);
begin
  if not THAMTNode.removeIfThere(@froot, key) then THAMTNode.raiseMissingKey(key);
  dec(fcount);
end;

procedure TMutableMap.clear;
begin
  THAMTNode.decrementRefCount(froot);
  froot := THAMTNode.allocate(0,0);
  fcount := 0;
end;


function TMutableMap.clone: TMutableMap;
begin
  result := TMutableMap.Create(self);
end;




constructor TImmutableMap.Create;
begin
  froot := THAMTNode.allocate(0,0);
  fcount := 0;
end;

constructor TImmutableMap.Create(other: specialize TReadOnlyMap<TKey, TValue, TInfo>);
begin
  fcount := other.fcount;
  froot := other.froot;
  InterLockedIncrement(froot.refCount);
end;

function TImmutableMap.insert(const key: TKey; const value: TValue; allowOverride: boolean): TImmutableMap;
begin
  result := TImmutableMap.Create(self);
  if THAMTNode.insert(@result.froot, key, value, allowOverride) then
    Inc(result.fcount);
end;

function TImmutableMap.remove(const key: TKey): TImmutableMap;
begin
  result := TImmutableMap.Create(self);
  if not THAMTNode.removeIfThere(@result.froot, key) then begin
    result.free;
    THAMTNode.raiseMissingKey(key);
  end;
  dec(result.fcount);
end;

function TImmutableMap.clone: TImmutableMap;
begin
  result := TImmutableMap.Create(self);
end;

end.

