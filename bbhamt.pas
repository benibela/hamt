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
  generic THAMTItemHelper<T> = record //can't be specialized in the THAMTNode interface https://bugs.freepascal.org/view.php?id=34232
    type
    TSizeEquivalent = packed array[1..sizeof(T)] of byte;
    PSizeEquivalent = ^TSizeEquivalent;
  end;
  //** @abstract(Low-level HAMT from which the maps are built)
  //** Each node has a reference counter and stores up to 32 pointers or item. @br
  //** The HAMT node is either mutable (if reference counter is 1) or immutable with copy-on-write (if reference counter is >= 2) like strings. @br
  //** Using the nodes directly would be more efficient than using the map classes, since you have one less memory access without the class instance.
  generic THAMTNode<TItem, TInfo> = packed object
    type
    PItem = ^TItem;
    {Invariants:
    - THAMTArray is not empty, count > 0
    - All keys in one THAMTArray have the same hash
    }
    PHAMTArray = ^THAMTArray;
    THAMTArray = packed object
      refCount: Integer;
      count: integer;
      firstItem: array[0..0] of TItem;
      class function size(aCount: integer): SizeInt; static; inline;
      class function allocate(acount: integer): PHAMTArray; static; //not initialized besides counts
      class procedure decrementRefCount(a: PHAMTArray); static;
      procedure incrementChildrenRefCount;
      function indexOf(const item: TItem): integer;
      function find(const item: TItem): PItem;
      function get(i: Integer): PItem; inline;
      property items[i: Integer]: PItem read get; default;
      //todo: this can easily be extended to a multi map, by having multiple pairs with the same key
    end;
    PHAMTNode = ^THAMTNode;
    PPHAMTNode = ^PHAMTNode;
    THAMTEnumerator = object
    protected
      level: integer;
      stack: array[0..LEVEL_HIGH] of PHAMTNode;
      offsets: array[0..LEVEL_HIGH] of integer;
      fcurrent, lastItem: PItem;
      function pushNode(node: PHAMTNode): boolean;
    public
      procedure initialize(node: PHAMTNode);
      property current: PItem read fcurrent;
      function MoveNext: boolean;
    end;

  protected
    refCount: Integer;
    pointerCount, itemCount: word;
    bitmapIsSinglePointer: THAMTBitmap;
    bitmapIsValue: THAMTBitmap;
    pointers: array[0..63] of THAMTTaggedPointer;
    function getPointerOffset(index: THAMTHash): DWord; inline;
    //function getPointer(index: THAMTHash): pointer; inline;
    function getItemOffset(index: THAMTHash): DWord; inline;
    function getItemFromOffset(offset: DWord): PItem; inline;
    function getItemAddr(index: THAMTHash): PItem; inline;
    function getNodeFromOffset(offset: DWord): PHAMTNode; inline;
    class procedure hashShift(var hash: THAMTHash; out index: THAMTHash); static; inline;
    class function size(apointerCount, aitemCount: integer): SizeInt; static; inline;
    class function allocate(apointerCount, aitemCount: integer): PHAMTNode; static; //memory not initialized besides counts
    procedure incrementChildrenRefCount;
  public
    class procedure decrementRefCount(node: PHAMTNode); static;
    class procedure decrementRefCountButKeepChildren(node: PHAMTNode); static;
    class function allocateEmpty(): PHAMTNode; static; //memory initialized
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
    class function insert(ppnode: PPHAMTNode; const item: TItem; allowOverride: boolean): Boolean; static;
    class function removeIfThere(ppnode: PPHAMTNode; const item: TItem): Boolean; static;
    function find(const item: TItem): PItem;
    function contains(const item: TItem): boolean;
  end;


  //** @abstract(Default hash function and reference counting for strings/objects)
  //** The HAMT requires a hash function (hash) and equality test (equal) to compare keys. @br
  //** Memory management requires methods for reference counting (addRef, release) . Reference counting is mandatory as the HAMT might make arbitrary many copies of everything. @br
  //** You can derive an object of THAMTTypeInfo to change some methods, e.g., the hashing.
  THAMTTypeInfo = object
    class function hash(const s: string): THAMTHash; static;
    class function equal(const s, t: string): boolean; static;

    class procedure addRef(var k: string); inline; static;
    class procedure release(var k: string); inline; static;
    class function toString(const k: string): string; static; inline;

    class procedure addRef(var k: IUnknown); static; inline;
    class procedure release(var k: IUnknown); static; inline;

    //no reference counting for objects
    class procedure addRef(var {%H-}k: TObject); inline;
    class procedure release(var {%H-}k: TObject); inline;
    class function toString(const k: TObject): string; inline;
  end;

  //** @abstract(Generic read-only map)
  //**
  //** The data in this map can be read, but there are no methods to modify it.
  generic TReadOnlyMap<TKey, TValue, TInfo> = class
    type
    PKey = ^TKey;
    PValue = ^TValue;
    TKeySizeEquivalent = packed array[1..sizeof(TKey)] of byte;
    TValueSizeEquivalent = packed array[1..sizeof(TValue)] of byte;

    TPair = packed record
      key: TKey;
      value: TValue;
    end;
    PPair = ^TPair;
    type TPairInfo = record
      class function hash(const p: TPair): THAMTHash; static; inline;
      class function equal(const p, q: TPair): boolean; static; inline;

      class procedure addRef(var p: TPair); static; inline;
      class procedure release(var p: TPair); static; inline;

      class procedure assignEqual(var p: TPair; const q: TPair); static; inline;
    end;

    THAMTNode = specialize THAMTNode<TPair, TPairInfo>;
    PHAMTNode = ^THAMTNode;
    private
      function removeIfThere(const key: TKey): boolean; inline;
    protected
      fcount: SizeInt;
      froot: PHAMTNode;
      class procedure raiseMissingKey(const key: TKey);
      function find(const key: TKey): PPair; inline;
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

class function TReadOnlyMap.TPairInfo.hash(const p: TPair): THAMTHash;
begin
  result := TInfo.hash(p.key);
end;

class function TReadOnlyMap.TPairInfo.equal(const p, q: TPair): boolean;
begin
  result := TInfo.equal(p.key, q.key);
end;

class procedure TReadOnlyMap.TPairInfo.addRef(var p: TPair);
begin
  TInfo.addRef(p.key);
  TInfo.addRef(p.value);
end;

class procedure TReadOnlyMap.TPairInfo.release(var p: TPair);
begin
  TInfo.release(p.key);
  TInfo.release(p.value);
end;

class procedure TReadOnlyMap.TPairInfo.assignEqual(var p: TPair; const q: TPair);
begin
  p.value := q.value;
end;


function THAMTNode.THAMTEnumerator.pushNode(node: PHAMTNode): boolean;
begin
  inc(level);
  stack[level] := node;
  offsets[level] := -1;
  result := node.itemCount > 0;
  if result then begin
    fcurrent := node.getItemFromOffset(0);
    lastItem := current + node.itemCount - 1;
  end;
end;

procedure THAMTNode.THAMTEnumerator.initialize(node: PHAMTNode);
begin
  level:=-1;
  if pushNode(node) then dec(fcurrent)
  else begin
    fcurrent := nil;
    lastItem := nil;
  end;
end;

function THAMTNode.THAMTEnumerator.MoveNext: boolean;
var
  node: PHAMTNode;
  p: Pointer;
  isArray: boolean;
begin
  result := current < lastItem;
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
        fcurrent := @PHAMTArray(p).firstItem[0];
        lastItem := fcurrent + PHAMTArray(p).count - 1;
        assert(fcurrent <= lastItem);
        exit(true);
      end;
      result := pushNode(PHAMTNode(p));
      if result then exit;
    end else dec(level);
  end;

  result := false;
end;


class function THAMTTypeInfo.hash(const s: string): THAMTHash;
begin
  result := objpas.hash(s);
end;

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
  result := 2*sizeof(integer) + sizeof(TItem) * acount
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
  p: PItem;
begin
  with a^ do begin
    p := @firstItem[0]; //use p[i] rather than pairs[i], because pairs[i] is not inlined
    if InterLockedDecrement(refCount) = 0 then begin
      for i := 0 to count - 1 do
        TInfo.release(p[i]);
      Freemem(a);
    end;
  end;
end;

procedure THAMTNode.THAMTArray.incrementChildrenRefCount;
var
  i: Integer;
  p: PItem;
begin
  p := @firstItem[0];
  for i := 0 to count - 1 do
    TInfo.addRef(p[i]);
end;

function THAMTNode.THAMTArray.indexOf(const item: TItem): integer;
var
  i: Integer;
  p: PItem;
begin
  p := @firstItem[0];
  for i := 0 to count - 1 do
    if TInfo.equal(p[i], item) then begin
      exit(i);
    end;
  result := -1;
end;

function THAMTNode.THAMTArray.find(const item: TItem): PItem;
var
  index: Integer;
begin
  index := indexOf(item);
  if index >= 0 then begin
    result :=  @firstItem[0];
    inc(result, index);
  end else result := nil;
end;

function THAMTNode.THAMTArray.get(i: Integer): PItem;
begin
  result := @firstItem[0];
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


function THAMTNode.getPointerOffset(index: THAMTHash): DWord;
begin
  result := bitmapIsSinglePointer.countBeforeIndex(index);
end;

function THAMTNode.getItemOffset(index: THAMTHash): DWord;
begin
  result := bitmapIsValue.countBeforeIndex(index);
end;

function THAMTNode.getItemFromOffset(offset: DWord): PItem;
begin
  result := PItem(@pointers[pointerCount]) + offset;
end;

function THAMTNode.getItemAddr(index: THAMTHash): PItem;
begin
  result := getItemFromOffset(getItemOffset(index));
end;

function THAMTNode.getNodeFromOffset(offset: DWord): PHAMTNode;
begin
  result := PHAMTNode(pointers[offset].raw);
end;


procedure THAMTNode.incrementChildrenRefCount;
var
  isArray: boolean;
  pointerRaw: Pointer;
  pairs: PItem;
  i: Integer;
begin
  for i := 0 to pointerCount - 1 do begin
    pointerRaw := pointers[i].unpack(isArray);
    if isArray then InterLockedIncrement(PHAMTArray(pointerRaw).refCount)
    else InterLockedIncrement(PHAMTNode(pointerRaw).refCount)
  end;
  pairs := PItem(@pointers[pointerCount]);
  for i := 0 to itemCount - 1 do
    TInfo.addRef(pairs[i]);
end;


class procedure THAMTNode.decrementRefCount(node: PHAMTNode);
var
  isArray: boolean;
  pointerRaw: Pointer;
  pairs: PItem;
  i: Integer;
begin
  with node^ do begin
    if InterLockedDecrement(refCount) = 0 then begin
      for i := 0 to pointerCount - 1 do begin
        pointerRaw := pointers[i].unpack(isArray);
        if isArray then THAMTArray.decrementRefCount(PHAMTArray(pointerRaw))
        else decrementRefCount(PHAMTNode(pointerRaw))
      end;
      pairs := PItem(@pointers[pointerCount]);
      for i := 0 to itemCount - 1 do
        TInfo.release(pairs[i]);
      Freemem(node);
    end;
  end;
end;

class procedure THAMTNode.decrementRefCountButKeepChildren(node: PHAMTNode);
begin
  if node.refCount <= 1 then Freemem(node)
  else begin
    node.incrementChildrenRefCount;
    decrementRefCount(node);
  end;
end;

class function THAMTNode.size(apointerCount, aitemCount: integer): SizeInt;
begin
  size := SizeOf(THAMTNode.refCount) + SizeOf(THAMTNode.pointerCount) + SizeOf(THAMTNode.itemCount) + SizeOf(THAMTNode.bitmapIsSinglePointer) + SizeOf(THAMTNode.bitmapIsValue)
                   + SizeOf(Pointer) * apointerCount
                   + SizeOf(TItem) * aitemCount;
end;

class function THAMTNode.allocate(apointerCount, aitemCount: integer): PHAMTNode;
var
  s: SizeInt;
begin
  s := size(apointerCount, aitemCount);
  result := alignedGetMem(s);
  result^.refCount := 1;
  result^.pointerCount := apointerCount;
  result^.itemCount := aitemCount;
end;

class function THAMTNode.allocateEmpty(): PHAMTNode;
const
  s: SizeInt = SizeOf(THAMTNode.refCount) + SizeOf(THAMTNode.pointerCount) + SizeOf(THAMTNode.itemCount) + SizeOf(THAMTNode.bitmapIsSinglePointer) + SizeOf(THAMTNode.bitmapIsValue);
begin
  result := alignedGetMem(s);
  result^.refCount := 1;
  result^.pointerCount := 0;
  result^.itemCount := 0;
  result^.bitmapIsSinglePointer.all := 0;
  result^.bitmapIsValue.all := 0;
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

    s := size(result.pointerCount, result.itemCount);
    ppnode^ := alignedGetMem(s); //new node
    move(result^, ppnode^^, s);
    decrementRefCount(result); //result is still the old node
    ppnode^^.refCount := 1;
    result := ppnode^;
  end;
end;


class function THAMTNode.insert(ppnode: PPHAMTNode; const item: TItem; allowOverride: boolean): Boolean;
var itemHelper: specialize THAMTItemHelper<TItem>;

var itemIndex: Integer;
  function cloneArrayAppend(hamtArrayx: pointer; append: boolean): pointer;
  var HAMTArray: PHAMTArray;
    oldItem: PItem;
  begin
    HAMTArray := PHAMTArray(hamtArrayx);
    if append then begin
      result := THAMTArray.allocate(hamtArray.count + 1);
      oldItem := PHAMTArray(result)^[hamtArray.count];
      itemHelper.PSizeEquivalent(oldItem)^ := itemHelper.PSizeEquivalent(@item)^;
      TInfo.addRef(oldItem^);
    end else begin
      result := THAMTArray.allocate(hamtArray.count);
    end;
    move(hamtArray^[0]^, PHAMTArray(result)^[0]^, hamtArray.count * sizeof(TItem));
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
  procedure moveItemsDown(itemsIsArray: boolean; items: pointer);
  var node : PHAMTNode;
      dataOffset: integer;
      oldItem: PItem;
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
        //create node of old items and new item
        if itemsIsArray then begin
          node := THAMTNode.allocate(1, 1);
          node.bitmapIsSinglePointer.all := THAMTHash(1) shl index2;
          node.bitmapIsValue.all := THAMTHash(1) shl index;
          node.pointers[0].setToArray(items);
          dataOffset := 0;
        end else begin
          node := THAMTNode.allocate(0, 2);
          node.bitmapIsSinglePointer.all := 0;
          node.bitmapIsValue.all := (THAMTHash(1) shl index) or (THAMTHash(1) shl index2);
          if index < index2 then dataOffset := 0 else dataOffset := 1;
          oldItem := node.getItemFromOffset(1 - dataOffset);
          itemHelper.PSizeEquivalent(oldItem)^ := itemHelper.PSizeEquivalent(items)^;
        end;
        oldItem := node.getItemFromOffset(dataOffset);
        itemHelper.PSizeEquivalent(oldItem)^ := itemHelper.TSizeEquivalent(item);
        TInfo.addRef(oldItem^);
        ppnode^ := node;
        exit;
      end;
    end;
    assert(false);
  end;

var
  node: PHAMTNode;
  itemOffset: DWord;
  oldItem: PItem;
  pointerIsArray: boolean;
  rawPointer: Pointer;
  hamtArray: PHAMTArray;
begin
  result := true;
  node := ppnode^;
  h := TInfo.hash(item);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);

    if node.bitmapIsSinglePointer.bits[index] then begin
      node := UniqueNode(ppnode);
      offset := node.getPointerOffset(index);
      rawPointer := node.pointers[offset].unpack(pointerIsArray);
      if pointerIsArray then begin
        hamtArray := PHAMTArray(rawPointer);
        h2 := TInfo.hash(hamtArray^[0]^) shr (BITS_PER_LEVEL * i + BITS_PER_LEVEL);
        if h <> h2 then begin
          //child at index is an array where the keys have a different hash
          //=> move array to a lower level on which the array hash and new key hash end up at a different index
          moveitemsDown(true, hamtArray);
        end else begin
          //array and new key have same hash => insert into array
          {Cases:
          key already in array           refCount = 1         => override value
          key already in array           refCount > 1         => clone array, override value in new array
          key not in array               refCount = 1         => resize array: move values from old to new array (do not change ref counts)
          key not in array               refCount > 1         => create larger array, copy values from old to new

          }
          itemIndex := hamtArray.indexOf(item);
          if (itemIndex >= 0) then begin
            result := false;
            if not allowOverride then exit;
          end;
          if (itemIndex < 0) or (hamtArray.refCount > 1) then begin
            hamtArray := cloneArrayAppend(hamtArray, itemIndex < 0);
            node.pointers[offset].setToArray(hamtArray);
          end;
          if itemIndex >= 0 then
            TInfo.assignEqual(hamtArray^[itemIndex]^, item);
        end;
        exit;
      end else begin
        //go to next level
        ppnode := @ppnode^.pointers[offset].raw;
        node := ppnode^;
      end;
    end else if node.bitmapIsValue.bits[index] then begin
      itemOffset := node.getItemOffset(index);
      oldItem := node.getItemFromOffset(itemOffset);
      if not tinfo.equal(oldItem^, item) then begin
        //change item to array pointer
        offset := node.getPointerOffset(index);
        ppnode^ := allocate(node.pointerCount + 1, node.itemCount - 1);
        //    [ ..head..   ..pointerPrefix..            ..pointerSuffix..    ..itemPrefix..   old item    ..itemSuffix.. ]
        // -> [ ..head..   ..pointerPrefix.. ..newPointer ..pointerSuffix..  ..itemPrefix..             ..itemSuffix.. ]
        move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, sizeof(THAMTBitmap) + sizeof(THAMTBitmap) + sizeof(Pointer) * offset); //head, pointer prefix
        move(node.pointers[offset], ppnode^.pointers[offset + 1], sizeof(pointer) * SizeUInt(node.pointerCount - offset) + sizeof(Titem) * itemOffset); //..pointerSuffix..    ..itemPrefix..
        move(node.getItemFromOffset(itemOffset + 1)^ , ppnode^.getItemFromOffset(itemOffset)^, (node.itemCount - itemOffset - 1) * sizeof(Titem) ); //..itemSuffix..
        ppnode^.bitmapIsSinglePointer.bits[index] := true;
        ppnode^.bitmapIsValue.bits[index] := False;
        result := true;
        if i < LEVEL_HIGH then begin
          h2 := TInfo.hash(oldItem^) shr (BITS_PER_LEVEL * i + BITS_PER_LEVEL);
          if h <> h2 then begin
            moveitemsDown(false, oldItem);
            result := false;
          end;
        end;
        if result then begin
          hamtArray := THAMTArray.allocate(2);
          itemHelper.PSizeEquivalent(hamtArray^[0])^ := itemHelper.PSizeEquivalent(oldItem)^;
          itemHelper.PSizeEquivalent(hamtArray^[1])^ := itemHelper.TSizeEquivalent(item);
          TInfo.addRef(hamtArray^[1]^);
          ppnode^.pointers[offset].setToArray(hamtArray);
        end else result := true;
        decrementRefCountButKeepChildren(node);
      end else begin
        result := false;
        if not allowOverride then exit;
        node := UniqueNode(ppnode);
        TInfo.assignEqual(node.getItemFromOffset(itemOffset)^, item);
      end;
      exit;
    end else begin
      //copy node and add key+value item
      offset := node.getItemOffset(index);
      ppnode^ := allocate(node.pointerCount, node.itemCount + 1);
      oldItem := ppnode^.getItemFromOffset(offset);
      itemHelper.PSizeEquivalent(oldItem)^ := itemHelper.TSizeEquivalent(item);
      TInfo.addRef(oldItem^);
      move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, 2*sizeof(THAMTBitmap) + sizeof(Pointer) * SizeUInt(node.pointerCount) + sizeof(Titem) * offset);
      move( node.getItemFromOffset(offset)^ , (oldItem + 1)^ , (node.itemCount - offset) * sizeof(Titem) );
      decrementRefCountButKeepChildren(node);
      ppnode^.bitmapIsValue.bits[index] := true;
      exit;
    end;
  end;
end;

class function THAMTNode.removeIfThere(ppnode: PPHAMTNode; const item: TItem): Boolean;
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
    while (level >= 0) and (nodes[level].itemCount = 0) and (nodes[level].pointerCount <= 1) do
      dec(level );

    if level < 0 then begin
      THAMTNode.decrementRefCount(initialPPNode^);
      initialPPNode^ := THAMTNode.allocateEmpty;
      exit;
    end;

    if level > 0 then begin
      parentNode := uniqueAncestorNodes(level - 1);
      node := parentNode.getNodeFromOffset(offsets[level - 1]);
    end else begin
      parentNode := nil;
      node := initialPPNode^;
    end;
    newNode := allocate(node.pointerCount - 1, node.itemCount);
    deleteOffset := offsets[level];
    //    [ ..head..   ..pointerPrefix pointer pointerSuffix..    ..items.. ]
    // -> [ ..head..   ..pointerPrefix pointerSuffix....  ]
    move(node.bitmapIsSinglePointer, newNode.bitmapIsSinglePointer, 2*sizeof(THAMTBitmap) + sizeof(Pointer) * deleteOffset);
    move(node.pointers[deleteOffset + 1] , newNode.pointers[deleteOffset],  (node.pointerCount - deleteOffset - 1) * sizeof(pointer) + node.itemCount * sizeof(Titem) );
    newNode.bitmapIsSinglePointer.bits[indices[level]] := False;
    p := node.pointers[deleteOffset].unpack(isArray);
    decrementRefCountButKeepChildren(node);
    if not isArray then THAMTNode.decrementRefCount(p) //need to decrement RC again in case decrementRefCountButKeepChildren incremented it
    else THAMTArray.decrementRefCount(p);
    if level > 0 then
      parentNode.pointers[offsets[level - 1]].raw := newNode
    else
      initialPPNode^ := newNode;
  end;

var
  hamtArray, newHamtArray: PHAMTArray;
  itemIndex: integer;
  i: Integer;
  node: PHAMTNode;
  h, index: THAMTHash;
  offset, itemOffset: DWord;
  rawPointer: Pointer;
  pointerIsArray: boolean;
  oldItem: PItem;
begin
  result := true;
  initialPPNode := ppnode;
  node := ppnode^;
  h := TInfo.hash(item);
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
        itemIndex := hamtArray.indexOf(item);
        if itemIndex < 0 then
          exit(false);
        if hamtArray.count = 1 then begin
          deletePointerFromNode(i);
          exit;
        end;
        //todo: optimize special case hamtArray.count = 2 by converting array to in-node value
        node := uniqueAncestorNodes(i);
        newHamtArray := THAMTArray.allocate(hamtArray.count - 1);
        move(hamtArray^[0]^, newHamtArray^[0]^, sizeof(Titem) * itemIndex );
        move(hamtArray^[itemIndex + 1]^, newHamtArray^[itemIndex]^, sizeof(Titem) * ( hamtArray.count - itemIndex - 1 ) );
        if hamtArray.refCount > 1 then begin
          newHamtArray.incrementChildrenRefCount;
          THAMTArray.decrementRefCount(hamtArray);
        end else begin
          TInfo.release(hamtArray^[itemIndex]^);
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
      itemOffset := node.getItemOffset(index);
      oldItem := node.getItemFromOffset(itemOffset);
      if not TInfo.equal(oldItem^, item) then begin
        //nothing to remove
        result := false;
      end else begin
        if (node.pointerCount = 0) and (node.itemCount = 1) then begin
          deletePointerFromNode(i - 1);
          exit;
        end;
        if i > 0 then
          ppnode := @PHAMTNode(uniqueAncestorNodes(i - 1)).pointers[offset].raw;
        //remove item
        //
        ppnode^ := allocate(node.pointerCount, node.itemCount - 1);
        //    [ ..head..   ..pointers..    ..itemPrefix..   old item    ..itemSuffix.. ]
        // -> [ ..head..   ..pointers....  ..itemPrefix..               ..itemSuffix.. ]
        move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, 2*sizeof(THAMTBitmap) + sizeof(Pointer) * SizeUInt(node.pointerCount) + sizeof(Titem) * itemOffset);
        move(node.getItemFromOffset(itemOffset + 1)^ , ppnode^.getItemFromOffset(itemOffset)^, (node.itemCount - itemOffset - 1) * sizeof(Titem) ); //..itemSuffix..
        if node.refCount > 1 then begin
          ppnode^.incrementChildrenRefCount;
          THAMTNode.decrementRefCount(node);
        end else begin
          TInfo.release(node^.getItemFromOffset(itemOffset)^);
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

function THAMTNode.find(const item: TItem): PItem;
var
  node: PHAMTNode;
  i: Integer;
  h, index: THAMTHash;
  rawPointer: Pointer;
  pointerIsArray: boolean;
  offset: THAMTHash;
begin
  node := @self;
  h := TInfo.hash(item);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);
    if node.bitmapIsSinglePointer.bits[index] then begin
      offset := node.getPointerOffset(index);
      rawPointer := node.pointers[offset].unpack(pointerIsArray);
      if pointerIsArray then
        exit(PHAMTArray(rawPointer).find(item))
       else
        node := PHAMTNode(rawPointer)
    end else if node.bitmapIsValue.bits[index] then begin
      result := node.getItemAddr(index);
      if not TInfo.equal(result^, item) then result := nil;
      exit;
    end else
      exit(nil);
  end;
  result := nil;
end;

function THAMTNode.contains(const item: TItem): boolean;
begin
  result := find(item) <> nil;
end;

function TReadOnlyMap.removeIfThere(const key: TKey): boolean;
begin
  result := THAMTNode.removeIfThere(@froot, PPair(@key)^ ); //this cast should work, because key is the first element of TPair
end;

class procedure TReadOnlyMap.raiseMissingKey(const key: TKey);
begin
  raise EHAMTKeyNotFound.Create('Key not found: '+TInfo.toString(key));
end;

function TReadOnlyMap.find(const key: TKey): PPair;
begin
  result := froot.find( PPair(@key)^ ); //this cast should work, because key is the first element of TPair
end;

function TReadOnlyMap.isEmpty: boolean;
begin
  result := count = 0;
end;

function TReadOnlyMap.contains(const key: TKey): boolean;
begin
  result := find(key) <> nil;
end;

function TReadOnlyMap.get(const key: TKey; const def: TValue): TValue;
var
  pair: PPair;
begin
  pair := find(key);
  if pair = nil then result := def
  else result := pair.value;
end;

function TReadOnlyMap.get(const key: TKey): TValue;
var
  pair: PPair;
begin
  pair := find(key);
  if pair = nil then raiseMissingKey(key);
  result := pair.value;
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
  froot := THAMTNode.allocateEmpty;
  fcount := 0;
end;

constructor TMutableMap.Create(other: specialize TReadOnlyMap<TKey, TValue, TInfo>);
begin
  fcount := other.fcount;
  froot := other.froot;
  InterLockedIncrement(froot.refCount);
end;

function TMutableMap.insert(const key: TKey; const value: TValue; allowOverride: boolean): boolean;
var tempPair: packed array[1..sizeof(TKey)+sizeof(TValue)] of byte;
begin
  TKeySizeEquivalent(PPair(@tempPair).key) := TKeySizeEquivalent(key);
  TValueSizeEquivalent(PPair(@tempPair).value) := TValueSizeEquivalent(value);

  result := THAMTNode.insert(@froot, PPair(@tempPair)^, allowOverride);
  if Result then Inc(fcount);
end;

procedure TMutableMap.remove(const key: TKey);
begin
  if not removeIfThere(key) then raiseMissingKey(key);
  dec(fcount);
end;

procedure TMutableMap.clear;
begin
  THAMTNode.decrementRefCount(froot);
  froot := THAMTNode.allocateEmpty;
  fcount := 0;
end;


function TMutableMap.clone: TMutableMap;
begin
  result := TMutableMap.Create(self);
end;




constructor TImmutableMap.Create;
begin
  froot := THAMTNode.allocateEmpty;
  fcount := 0;
end;

constructor TImmutableMap.Create(other: specialize TReadOnlyMap<TKey, TValue, TInfo>);
begin
  fcount := other.fcount;
  froot := other.froot;
  InterLockedIncrement(froot.refCount);
end;

function TImmutableMap.insert(const key: TKey; const value: TValue; allowOverride: boolean): TImmutableMap;
var tempPair: packed array[1..sizeof(TKey)+sizeof(TValue)] of byte;
begin
  TKeySizeEquivalent(PPair(@tempPair).key) := TKeySizeEquivalent(key);
  TValueSizeEquivalent(PPair(@tempPair).value) := TValueSizeEquivalent(value);

  result := TImmutableMap.Create(self);
  if THAMTNode.insert(@result.froot, PPair(@tempPair)^, allowOverride) then
    Inc(result.fcount);
end;

function TImmutableMap.remove(const key: TKey): TImmutableMap;
begin
  result := TImmutableMap.Create(self);
  if not result.removeIfThere(key) then begin
    result.free;
    raiseMissingKey(key);
  end;
  dec(result.fcount);
end;

function TImmutableMap.clone: TImmutableMap;
begin
  result := TImmutableMap.Create(self);
end;

end.

procedure THAMTNode.TPair.init(const k: TKey; const v: TValue);
begin
  TKeySizeEquivalent(key) := TKeySizeEquivalent(k);
  TValueSizeEquivalent(value) := TValueSizeEquivalent(v);
  addRef();
end;

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

procedure init(const k: TKey; const v: TValue); inline;
    class procedure assignKeyRef(var target: TKey; const source: TKey); static; inline;
    class procedure assignValueRef(var target: TValue; const source: TValue); static; inline;
        function get(const key: TItem; const def: TValue): TValue;

        function get(const key: TItem): TValue;

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

