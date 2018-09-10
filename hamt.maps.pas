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
@abstract(Mutable and immutable persistent maps as hash array mapped trie (HAMT))

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
unit hamt.maps;

{$mode objfpc}{$H+}{$ModeSwitch autoderef}{$ModeSwitch advancedrecords}

interface

uses
  sysutils, hamt.internals;

type
THAMTTypeInfo = hamt.internals.THAMTTypeInfo;

generic THAMTPairInfo<TKey, TValue, TInfo> = record
  type
  TPair = packed record
    key: TKey;
    value: TValue;
  end;

  class function hash(const p: TPair): THAMTHash; static; inline;
  class function equal(const p, q: TPair): boolean; static; inline;

  class procedure addRef(var p: TPair); static; inline;
  class procedure release(var p: TPair); static; inline;

  class procedure assignEqual(var p: TPair; const q: TPair); static; inline;

  class function toString(const p: TPair): string; static; inline;
end;

//** @abstract(Generic read-only map)
//**
//** The data in this map can be read, but there are no public methods to modify it.
generic TReadOnlyMap<TKey, TValue, TInfo> = class(specialize TReadOnlyCustomSet<specialize THAMTPairInfo<TKey, TValue, TInfo>.TPair, specialize THAMTPairInfo<TKey, TValue, TInfo>>)
  type
  PKey = ^TKey;
  PValue = ^TValue;
  TKeySizeEquivalent = packed array[1..sizeof(TKey)] of byte;
  TValueSizeEquivalent = packed array[1..sizeof(TValue)] of byte;
  PPair = THAMTNode.PItem;
  private
    function forceInclude(const key: TKey; const value: TValue; allowOverride: boolean): boolean; inline;
    function forceExclude(const key: TKey): boolean; inline;
  protected
    function find(const key: TKey): PPair; inline;
    class procedure raiseKeyError(const message: string; const key: TKey); static;
  public
    //** Creates an empty map
    constructor Create;
    //** Creates a map equal to other. No data is copied, till either map is modified (copy-on-write).
    constructor Create(other: specialize TReadOnlyCustomSet<THAMTNode.TItem, THAMTNode.TInfo>);
    //** Returns if the map contains a certain key
    function contains(const key:TKey): boolean; inline;
    //** Returns the value for a certain key, or default value def if the map does not contain the key
    function get(const key: TKey; const def: TValue): TValue; inline;
    //** Returns the value for a certain key, or raises an exception if the map does not contain the key
    function get(const key: TKey): TValue; inline;
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
protected
  procedure includeItem(const key: TKey; const value: TValue); inline;
public
  //** Inserts a (key, value) pair, if allowOverride is true or key did not exist
  //** @returns If the map did not contain key
  function include(const key: TKey; const value: TValue; allowOverride: boolean = true): boolean; inline;
  //** Removes a (key, value) pair
  //** @returns If the map did contain key
  function exclude(const key: TKey): boolean; inline;
  //** Inserts a (key, value) pair, or raises an exception if the map did not contain key
  procedure insert(const key: TKey; const value: TValue); inline;
  //** Removes key (and the associated value), or raises an exception if the map did not contain key
  procedure remove(const key:TKey); inline;
  //** Removes everything from the map;
  procedure clear;
  //** Creates a new map equal to self. No data is copied, till either map is modified (copy-on-write).
  function clone: TMutableMap;
  //** Default parameter, so you can read or write elements with @code(map[key])
  property items[key: TKey]: TValue read get write includeItem; default;
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
  //** Creates a new map containing (key, value). If the map does not contain key or allowOverride is true, the value associated with the key is @code(value), otherwise the value is unchanged.
  //** @returns The new map
  function include(const key: TKey; const value: TValue; allowOverride: boolean = true): TImmutableMap; inline; overload;
  //** Creates a new map without key and its associated value
  //** @returns The new map
  function exclude(const key: TKey): TImmutableMap; inline;
  //** Creates a new map containing (key, value), or raises an exception if the map already contained key
  //** @returns The new map
  function insert(const key: TKey; const value: TValue): TImmutableMap; inline;
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


implementation


class function THAMTPairInfo.hash(const p: TPair): THAMTHash;
begin
  result := TInfo.hash(p.key);
end;

class function THAMTPairInfo.equal(const p, q: TPair): boolean;
begin
  result := TInfo.equal(p.key, q.key);
end;

class procedure THAMTPairInfo.addRef(var p: TPair);
begin
  with p do begin
    TInfo.addRef(key);
    TInfo.addRef(value);
  end;
end;

class procedure THAMTPairInfo.release(var p: TPair);
begin
  with p do begin
    TInfo.release(key);
    TInfo.release(value);
  end;
end;

class procedure THAMTPairInfo.assignEqual(var p: TPair; const q: TPair);
begin
  p.value := q.value;
end;

class function THAMTPairInfo.toString(const p: TPair): string;
begin
  result := TInfo.toString(p.key);
end;

constructor TReadOnlyMap.Create;
begin
  froot := THAMTNode.allocateEmpty;
  fcount := 0;
end;

constructor TReadOnlyMap.Create(other: specialize TReadOnlyCustomSet<THAMTNode.TItem, THAMTNode.TInfo>);
begin
  fcount := other.fcount;
  froot := other.froot;
  InterLockedIncrement(froot.refCount);
end;

function TReadOnlyMap.forceInclude(const key: TKey; const value: TValue; allowOverride: boolean): boolean;
var tempPair: packed array[1..sizeof(TKey)+sizeof(TValue)] of byte;
begin
  TKeySizeEquivalent(PPair(@tempPair).key) := TKeySizeEquivalent(key);
  TValueSizeEquivalent(PPair(@tempPair).value) := TValueSizeEquivalent(value);
  result := THAMTNode.include(@froot, PPair(@tempPair)^, allowOverride);
  if result then inc(fcount);
end;

function TReadOnlyMap.forceExclude(const key: TKey): boolean;
begin
  result := THAMTNode.exclude(@froot, PPair(@key)^ ); //this cast should work, because key is the first element of TPair
  if result then dec(fcount);
end;


function TReadOnlyMap.find(const key: TKey): PPair;
begin
  result := froot.find( PPair(@key)^ ); //this cast should work, because key is the first element of TPair
end;

class procedure TReadOnlyMap.raiseKeyError(const message: string; const key: TKey);
var s: string;
begin
  s := TInfo.toString(key);
  raise EHAMTException.Create(Format(message, [s]) );
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
  if pair = nil then
    raiseKeyError(rsMissingKey, key);
  result := pair.value;
end;



procedure TMutableMap.includeItem(const key: TKey; const value: TValue);
begin
  forceInclude(key, value, true);
end;

function TMutableMap.include(const key: TKey; const value: TValue; allowOverride: boolean): boolean;
begin
  result := forceInclude(key, value, allowOverride);
end;
function TMutableMap.exclude(const key: TKey): boolean;
begin
  result := forceExclude(key);
end;
procedure TMutableMap.insert(const key: TKey; const value: TValue);
begin
  if not forceInclude(key, value, false) then raiseKeyError(rsDuplicateKey, key);
end;
procedure TMutableMap.remove(const key:TKey);
begin
  if not forceExclude(key) then raiseKeyError(rsMissingKey, key);
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


function TImmutableMap.include(const key: TKey; const value: TValue; allowOverride: boolean): TImmutableMap; inline;
begin
  result := TImmutableMap.Create(self);
  result.forceInclude(key, value, allowOverride)
end;
function TImmutableMap.exclude(const key: TKey): TImmutableMap; inline;
begin
  result := TImmutableMap.Create(self);
  result.forceExclude(key);
end;
function TImmutableMap.insert(const key: TKey; const value: TValue): TImmutableMap; inline;
begin
  result := TImmutableMap.Create(self);
  if not result.forceInclude(key, value, false) then begin
    result.free;
    raiseKeyError(rsDuplicateKey, key);
  end;
end;
function TImmutableMap.remove(const key:TKey): TImmutableMap; inline;
begin
  result := TImmutableMap.Create(self);
  if not result.forceExclude(key) then begin
    result.free;
    raiseKeyError(rsMissingKey, key);
  end;
end;


function TImmutableMap.clone: TImmutableMap;
begin
  result := TImmutableMap.Create(self);
end;


end.

