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
@abstract(Low-level HAMT)


}
unit hamt.internals;

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
  EHAMTException = class(Exception);
  generic THAMTItemHelper<T> = record //can't be specialized in the THAMTNode interface https://bugs.freepascal.org/view.php?id=34232
    type
    TSizeEquivalent = packed array[1..sizeof(T)] of byte;
    PSizeEquivalent = ^TSizeEquivalent;
  end;
  //** @abstract(Low-level HAMT from which further collections are built)
  //** Each node has a reference counter and stores up to 32 pointers or items. @br
  //** The HAMT node is either mutable (if reference counter is 1) or immutable with copy-on-write (if reference counter is >= 2) like strings. @br
  //** Using the nodes directly would be more efficient than using the map classes, since you have one less memory access without the class instance.
  generic THAMTNode<TItem, TInfo> = packed object
    const
      HAMTArrayEnd = -1;
    type
    PItem = ^TItem;
    {Invariants:
    - THAMTArray is not empty, count > 0
    - All keys in one THAMTArray have the same hash
    }
    PHAMTArray = ^THAMTArray;
    THAMTArrayIndex = integer;
    THAMTArray = packed object
      refCount: Integer;
      count: THAMTArrayIndex;
      firstItem: array[0..0] of TItem;
      class function size(aCount: THAMTArrayIndex): SizeInt; static; inline;
      class function allocate(acount: THAMTArrayIndex): PHAMTArray; static; //not initialized besides counts
      class procedure decrementRefCount(a: PHAMTArray); static;
      procedure incrementChildrenRefCount;
      function indexOf(const item: TItem): THAMTArrayIndex;
      function find(const item: TItem): PItem;
      function get(i: THAMTArrayIndex): PItem; inline;
      property items[i: THAMTArrayIndex]: PItem read get; default;
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
    class function size(apointerCount, aitemCount: SizeInt): SizeInt; static; inline;
    class function allocate(apointerCount, aitemCount: SizeInt): PHAMTNode; static; //memory not initialized besides counts
    procedure incrementChildrenRefCount;
  public
    class procedure decrementRefCount(node: PHAMTNode); static;
    class procedure decrementRefCountButKeepChildren(node: PHAMTNode); static;
    class function allocateEmpty(): PHAMTNode; static; //memory initialized
  public
    //trigger copy-on-write so the node becomes mutable
    class function uniqueNode(ppnode: PPHAMTNode): PHAMTNode; static;
    {
    //insert override allowed = true
      result = true                  inserted, no override
      result = false                 inserted, override
    //insert override forbidden = false
      result = true                  inserted, (no override)
      result = false                 not inserted
    }
    class function include(ppnode: PPHAMTNode; const item: TItem; allowOverride: boolean): Boolean; static;
    class function exclude(ppnode: PPHAMTNode; const item: TItem): Boolean; static;
    function find(const item: TItem): PItem;
    class function findAndUnique(ppnode: PPHAMTNode; const item: TItem): PItem; static;
    function contains(const item: TItem): boolean;
  end;


  //** @abstract(Default hash function and reference counting for strings/objects)
  //** The HAMT requires a hash function (hash) and equality test (equal) to compare keys. @br
  //** Memory management requires methods for reference counting (addRef, release) . Reference counting is mandatory as the HAMT might make arbitrary many copies of everything. assignEqual is called to assign one item to an equal, but not identical item. @br
  //** You can derive an object of THAMTTypeInfo to change some methods, e.g., the hashing.
  THAMTTypeInfo = object
    class function hash(const s: string): THAMTHash; static; inline;
    class function equal(const s, t: string): boolean; static; inline;

    class procedure addRef(var k: string); inline; static;
    class procedure release(var k: string); inline; static;
    class function toString(const k: string): string; static; inline;
    class procedure assignEqual(var a: string; const b: string); inline; static;

    class procedure addRef(var k: IUnknown); static; inline;
    class procedure release(var k: IUnknown); static; inline;
    class procedure assignEqual(var a: IUnknown; const b: IUnknown); inline; static;

    //no reference counting for objects
    class procedure addRef(var {%H-}k: TObject); static; inline;
    class procedure release(var {%H-}k: TObject); static;  inline;
    class function toString(const k: TObject): string; inline;
    class procedure assignEqual(var a: TObject; const b: TObject); inline; static;

    class procedure addRef(var {%H-}k: pointer); static; inline;
    class procedure release(var {%H-}k: pointer); static; inline;
    class function toString(const k: pointer): string; inline;
    class procedure assignEqual(var a: pointer; const b: pointer); inline; static;
  end;

  //** @abstract(Generic low-level read-only set)
  //**
  //** Data in this set can be read, but there are no methods to modify it.
  generic TReadOnlyCustomSet<TItem, TInfo> = class
    type
    THAMTNode = specialize THAMTNode<TItem, TInfo>;
    PHAMTNode = ^THAMTNode;
    protected
      fcount: SizeUInt;
      froot: PHAMTNode;
      class procedure raiseItemError(const message: string; const item: TItem);
    public
      //** Returns if the set is empty
      function isEmpty: boolean; inline;
      //** Enumerates all items in an unspecified order @br
      //** Example: @longcode(#
      //** var p: TReadOnlyCustomSet.PItem;
      //** for p in customSet do
      //**    ..
      //** #)
      //**
      //**The pointer let's you modify the item, but you must not modify it.
      function getEnumerator: THAMTNode.THAMTEnumerator;
      destructor Destroy; override;
      //** Number of items in the set
      property count: SizeUInt read fcount;
  end;




  function alignedGetMem(s: PtrUInt): pointer; inline;

{$ifdef FPC} //hide this from pasdoc, since it cannot parse external
  //need this in interface, otherwise THAMTTypeInfo.addRef/release is not inlined
  Procedure fpc_AnsiStr_Incr_Ref (S : Pointer); [external name 'FPC_ANSISTR_INCR_REF'];
  Procedure fpc_ansistr_decr_ref (Var S : Pointer); [external name 'FPC_ANSISTR_DECR_REF'];
{$endif}

  resourcestring
    rsMissingItem = 'Missing item: %s';
    rsDuplicateItem = 'Duplicate item: %s';
    rsMissingKey = 'Missing key: %s';
    rsDuplicateKey = 'Duplicate key: %s';

implementation



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

class procedure THAMTTypeInfo.assignEqual(var a: string; const b: string);
begin
  a := b;
end;

class procedure THAMTTypeInfo.addRef(var k: IUnknown);
begin
  k._AddRef;
end;

class procedure THAMTTypeInfo.release(var k: IUnknown);
begin
  k._Release;
end;

class procedure THAMTTypeInfo.assignEqual(var a: IUnknown; const b: IUnknown);
begin
  a := b;
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

class procedure THAMTTypeInfo.assignEqual(var a: TObject; const b: TObject);
begin
  a := b;
end;

class procedure THAMTTypeInfo.addRef(var k: pointer);
begin
  //empty
end;

class procedure THAMTTypeInfo.release(var k: pointer);
begin
  //empty
end;

class function THAMTTypeInfo.toString(const k: pointer): string;
begin
  result := IntToHex(PtrUInt(k), sizeof(k)*2);
end;

class procedure THAMTTypeInfo.assignEqual(var a: pointer; const b: pointer);
begin
  a := b
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
  mask := THAMTHash((THAMTHash(1) shl index) - 1);
  result := PopCnt(all and mask);
end;



class function THAMTNode.THAMTArray.size(aCount: THAMTArrayIndex): SizeInt;
begin
  result := sizeof(integer) + sizeof(THAMTArrayIndex) + sizeof(TItem) * acount
end;

class function THAMTNode.THAMTArray.allocate(acount: THAMTArrayIndex): PHAMTArray;
begin
  result := alignedGetMem(size(acount)) ;
  result.refCount := 1;
  result.count := acount;
end;

class procedure THAMTNode.THAMTArray.decrementRefCount(a: PHAMTArray);
var
  p, e: PItem;
begin
  with a^ do begin
    if InterLockedDecrement(refCount) = 0 then begin
      p := @firstItem[0]; //use p[i] rather than pairs[i], because pairs[i] is not inlined
      e := p + count;
      while p < e do begin
        TInfo.release(p^);
        inc(p);
      end;
      Freemem(a);
    end;
  end;
end;

procedure THAMTNode.THAMTArray.incrementChildrenRefCount;
var
  p, e: PItem;
begin
  p := @firstItem[0];
  e := p + count;
  while p < e do begin
    TInfo.addRef(p^);
    inc(p);
  end;
end;

function THAMTNode.THAMTArray.indexOf(const item: TItem): THAMTArrayIndex;
var
  p: PItem;
  i: THAMTArrayIndex;
begin
  p := @firstItem[0];
  i := 0;
  while i < count do begin
    if TInfo.equal(p[i], item) then
      exit(i);
    inc(i);
  end;
  result := HAMTArrayEnd;
end;

function THAMTNode.THAMTArray.find(const item: TItem): PItem;
var
  p, e: PItem;
begin
  p := @firstItem[0];
  e := p + count;
  while p < e do begin
    if TInfo.equal(p^, item) then
      exit(p);
    inc(p);
  end;
  result := nil;
end;

function THAMTNode.THAMTArray.get(i: THAMTArrayIndex): PItem;
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
  i: SizeInt;
begin
  for i := 0 to SizeInt(pointerCount) - 1 do begin
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
  i: SizeInt;
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

class function THAMTNode.size(apointerCount, aitemCount: SizeInt): SizeInt;
begin
  size := SizeOf(THAMTNode.refCount) + SizeOf(THAMTNode.pointerCount) + SizeOf(THAMTNode.itemCount) + SizeOf(THAMTNode.bitmapIsSinglePointer) + SizeOf(THAMTNode.bitmapIsValue)
                   + SizeOf(Pointer) * apointerCount
                   + SizeOf(TItem) * aitemCount;
end;

class function THAMTNode.allocate(apointerCount, aitemCount: SizeInt): PHAMTNode;
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
  s: SizeUInt = SizeOf(THAMTNode.refCount) + SizeOf(THAMTNode.pointerCount) + SizeOf(THAMTNode.itemCount) + SizeOf(THAMTNode.bitmapIsSinglePointer) + SizeOf(THAMTNode.bitmapIsValue);
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


class function THAMTNode.include(ppnode: PPHAMTNode; const item: TItem; allowOverride: boolean): Boolean;
var itemHelper: specialize THAMTItemHelper<TItem>;

var itemIndex: THAMTArrayIndex;
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
  i: SizeInt;
  h, index, h2: THAMTHash;
  offset: DWord;
  procedure moveItemsDown(itemsIsArray: boolean; items: pointer);
  var node : PHAMTNode;
      dataOffset: SizeInt;
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
          if (itemIndex <> HAMTArrayEnd) then begin
            result := false;
            if not allowOverride then exit;
          end;
          if (itemIndex = HAMTArrayEnd) or (hamtArray.refCount > 1) then begin
            hamtArray := cloneArrayAppend(hamtArray, itemIndex = HAMTArrayEnd);
            node.pointers[offset].setToArray(hamtArray);
          end;
          if itemIndex <> HAMTArrayEnd then
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

class function THAMTNode.exclude(ppnode: PPHAMTNode; const item: TItem): Boolean;
var
  initialPPNode: PPHAMTNode;
  indices:  array[0..LEVEL_HIGH] of THAMTHash;
  offsets:  array[0..LEVEL_HIGH] of SizeInt; //offsets[i] := ... getPointerOffset(indices[i])
  nodes:  array[0..LEVEL_HIGH] of PHAMTNode; //nodes[0] := ppnode^; nodes[i] := nodes[i-1].pointers[offsets[i-1]].raw

  //make sure nodes[0]..nodes[tillLevel] have ref count 0
  function uniqueAncestorNodes(tillLevel: SizeInt): pointer;
  var
    i, offset: SizeInt;
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
  itemIndex: THAMTArrayIndex;
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
        if itemIndex = HAMTArrayEnd then
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

class function THAMTNode.findAndUnique(ppnode: PPHAMTNode; const item: TItem): PItem; static;
var
  node: PHAMTNode;
  i: Integer;
  h, index: THAMTHash;
  rawPointer: Pointer;
  pointerIsArray: boolean;
  offset: THAMTHash;
begin
  node := uniqueNode(ppnode);
  h := TInfo.hash(item);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);
    if node.bitmapIsSinglePointer.bits[index] then begin
      offset := node.getPointerOffset(index);
      rawPointer := node.pointers[offset].unpack(pointerIsArray);
      if pointerIsArray then
        exit(PHAMTArray(rawPointer).find(item))
       else
        node := uniqueNode( PPHAMTNode(@node.pointers[offset].raw) );
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




class procedure TReadOnlyCustomSet.raiseItemError(const message: string; const item: TItem);
var s: string;
begin
  s := TInfo.toString(item);
  raise EHAMTException.Create(Format(message, [s]) );
end;

function TReadOnlyCustomSet.isEmpty: boolean;
begin
  result := count = 0;
end;

function TReadOnlyCustomSet.getEnumerator: THAMTNode.THAMTEnumerator;
begin
  result.initialize(froot);
end;

destructor TReadOnlyCustomSet.Destroy;
begin
  THAMTNode.decrementRefCount(froot);
  inherited;
end;





end.


