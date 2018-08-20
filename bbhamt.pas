unit bbhamt;

{$mode objfpc}{$H+}{$ModeSwitch autoderef}

interface

uses
  Classes, SysUtils;

type {generic THAMT<TKey, TValue, Thash>=class

end;  }
  TKey = string;
  TValue = string;
  PKey = ^TKey;
  PValue = ^TValue;
  TPair = packed record
    key: TKey;
    value: TValue;
  end;
  PPair = ^TPair;
  TPairArray = array of TPair;

  THAMTHash = Cardinal;
  THAMTBitmap = record
    case boolean of
      false: (bits: bitpacked array[0..31] of boolean);
      true: (all: Cardinal);
  end;
  { 4 cases                                           bitmapIsSinglePointer                     bitmapIsValue
     - next hamt node (pointer)                             true                                   false
     - list (pointer to multiple keys + values)             true                                   true
     - empty                                                false                                  false
     - one pair (key + value)                               false                                  true
  }
  PHAMTNode = ^THAMTNode;
  PPHAMTNode = ^PHAMTNode;
  THAMTNode = packed object
  protected
    type THAMTArray = packed object
      refCount: Integer;
      count: integer;
      data: array[0..0] of TPair;
    end;

    protected
    refCount: Integer;
    pointerCount, pairCount: word;
    bitmapIsSinglePointer: THAMTBitmap;
    bitmapIsValue: THAMTBitmap;
    pointers: array[0..63] of pointer;
    function getPointerOffset(index: THAMTHash): DWord; inline;
    function getPointer(index: THAMTHash): pointer; inline;
    function getPairOffset(index: THAMTHash): DWord; inline;
    function getPairFromOffset(offset: DWord): PPair; inline;
    function getPairAddr(index: THAMTHash): PPair; inline;
    class function findInArray(const pa: TPairArray; const key: TKey): PPair; static;
    procedure incrementChildrenRefCount;
    class function size(apointerCount, apairCount: integer): SizeInt; static; inline;
  public
    class function allocate(apointerCount, apairCount: integer): PHAMTNode; static;
    class procedure decrementRefCount(node: PHAMTNode); static;
  public
    class procedure insert(ppnode: PPHAMTNode; const key: TKey; const value: TValue; out overriden: boolean); static;
    function contains(const key:TKey):boolean;
    function get(const key: TKey; const def: TValue): TValue;
  end;

{  THAMTStringString = class

  end;}


implementation

const LEVEL_HIGH = 5;



function hash(const s: RawByteString): THAMTHash;
var
  p, last: PByte;
begin
  if s = '' then exit(1);
  case s of
    'test': exit(hash('collision'));
  end;
  p := pbyte(pointer(s));
  last := p + length(s);
  result := 0;
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

function equals(const s,t:string): boolean;
begin
  result := s = t;
end;

procedure hashShift(var hash: THAMTHash; out index: THAMTHash); inline;
begin
  index := hash and %11111;
  hash := hash shr 5;
end;

function bitmapCountBeforeIndex(const bitmapAll: Cardinal; index: THAMTHash): DWord; inline;
var
  mask: THAMTHash;
begin
  mask := (1 shl index) - 1;
  result := PopCnt(bitmapAll and mask);
end;

function THAMTNode.getPointerOffset(index: THAMTHash): DWord;
begin
  result := bitmapCountBeforeIndex(bitmapIsSinglePointer.all, index);
end;

function THAMTNode.getPointer(index: THAMTHash): pointer;
begin
  result := pointers[getPointerOffset(index)];
end;

function THAMTNode.getPairOffset(index: THAMTHash): DWord;
begin
  result := bitmapCountBeforeIndex(bitmapIsValue.all and not bitmapIsSinglePointer.all, index);
end;

function THAMTNode.getPairFromOffset(offset: DWord): PPair;
begin
  result := PPair(@pointers[pointerCount]) + offset;
end;

function THAMTNode.getPairAddr(index: THAMTHash): PPair;
begin
  result := getPairFromOffset(getPairOffset(index));
end;

class function THAMTNode.findInArray(const pa: TPairArray; const key: TKey): PPair;
var
  i: Integer;
begin
  for i := 0 to high(pa) do
    if equals(pa[i].key, key) then begin
      result := @pa[i];
      exit;
    end;
  result := nil;
end;

procedure THAMTNode.incrementChildrenRefCount;
begin

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
  result := GetMem(s);
  FillChar(result^, s, 0);
  result^.refCount := 1;
  result^.pointerCount := apointerCount;
  result^.pairCount := apairCount;
end;

class procedure THAMTNode.decrementRefCount(node: PHAMTNode);
begin
  InterLockedDecrement(node.refCount);
  if node.refCount <= 0 then begin
    //free children
    Freemem(node);
  end;
end;

class procedure THAMTNode.insert(ppnode: PPHAMTNode; const key: TKey; const value: TValue; out overriden: boolean);
var node: PHAMTNode;
  procedure cloneNode;
  var
    s: SizeInt;
  begin
    if node.refCount > 1 then begin
      //clone node so that it can be modified in the next iteration
      s := size(node.pointerCount, node.pairCount);
      ppnode^ := GetMem(s);
      move(node^, ppnode^^, s);
      InterLockedDecrement(node^.refCount);
      ppnode^^.refCount := 1;
      node := ppnode^;
    end;
  end;

var
  i: Integer;
  h, index: THAMTHash;
  offset, pairOffset: DWord;
  pair: PPair;
begin
  overriden := false;
  node := ppnode^;
  h := hash(key);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);
    {          [ refcount = 2 ]                 =>          [ refcount = 1 ]            [ refcount = 1 ]
     [ refcount = 1 ]        [ refcount = 1 ]                  [ refcount = 2 ]        [ refcount = 2 ]
     }
    if node.refCount > 1 then node.incrementChildrenRefCount;
    if node.bitmapIsValue.bits[index] then begin
      if node.bitmapIsSinglePointer.bits[index] then begin
        //insert into array
        offset := node.getPointerOffset(index);
        pair := findInArray( TPairArray(node.pointers[offset]), key );
        if pair <> nil then begin
          //todo array ref count
          overriden := true;
        end else begin
          cloneNode;
          SetLength(TPairArray(node.pointers[offset]), length(TPairArray(node.pointers[offset])) + 1);
          pair := @TPairArray(node.pointers[offset])[high(TPairArray(node.pointers[offset]))];
          pair^.key := key;
        end;
      end else begin
        pairOffset := node.getPairOffset(index);
        pair := node.getPairFromOffset(pairOffset);
        if pair.key <> key then begin
          //change pair to array pointer
          offset := node.getPointerOffset(index);
          ppnode^ := allocate(node.pointerCount + 1, node.pairCount - 1);
          //    [ ..head..   ..pointerPrefix..            ..pointerSuffix..    ..pairPrefix..   old pair    ..pairSuffix.. ]
          // -> [ ..head..   ..pointerPrefix.. ..newPointer ..pointerSuffix..  ..pairPrefix..             ..pairSuffix.. ]
          move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, sizeof(THAMTBitmap) + sizeof(THAMTBitmap) + sizeof(Pointer) * offset); //head, pointer prefix
          move(node.pointers[offset], ppnode^.pointers[offset + 1], sizeof(pointer) * (node.pointerCount - offset) + sizeof(TPair) * pairOffset); //..pointerSuffix..    ..pairPrefix..
          move(node.getPairFromOffset(pairOffset + 1)^ , ppnode^.getPairFromOffset(pairOffset)^, (node.pairCount - pairOffset - 1) * sizeof(TPair) ); //..pairSuffix..
          ppnode^.bitmapIsSinglePointer.bits[index] := true;
          ppnode^.pointers[offset] := nil;
          SetLength(TPairArray(ppnode^.pointers[offset]), 2); //todo memory leak?
          pair := @TPairArray(ppnode^.pointers[offset])[0];
          pair^ := node.getPairFromOffset(pairOffset)^;
          inc(pair);
          pair^.key := key;
          decrementRefCount(node);
        end;
      end;
      pair^.value := value;
      exit
    end else if node.bitmapIsSinglePointer.bits[index] then begin
      //go to next level
      cloneNode;
      ppnode := @ppnode^.pointers[ppnode^.getPointerOffset(index)];
      node := ppnode^;
    end else begin
      //add key+value pair to node
      offset := node.getPairOffset(index);
      ppnode^ := allocate(node.pointerCount, node.pairCount + 1);
      move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, sizeof(THAMTBitmap) + sizeof(THAMTBitmap) + sizeof(Pointer) * node.pointerCount + sizeof(TPair) * offset);
      move( node.getPairFromOffset(offset)^ , ppnode^.getPairFromOffset(offset+1)^ , (node.pairCount - offset) * sizeof(TPair) );
      pair := ppnode^.getPairFromOffset(offset);
      pair.key := key;
      pair.value := value;
      decrementRefCount(node);
      ppnode^.bitmapIsValue.bits[index] := true;
      exit;
    end;
  end;
end;

function THAMTNode.contains(const key: TKey): boolean;
var
  node: PHAMTNode;
  i: Integer;
  h, index: THAMTHash;
begin
  node := @self;
  h := hash(key);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);
    if node.bitmapIsValue.bits[index] then begin
      if node.bitmapIsSinglePointer.bits[index] then
        result := findInArray( TPairArray(getPointer(index)), key ) <> nil
      else
        result := equals(node.getPairAddr(index).key, key);
      exit
    end else if node.bitmapIsSinglePointer.bits[index] then
      node := PHAMTNode(getPointer(index))
    else
      exit(false);
  end;
  result := false;
end;

function THAMTNode.get(const key: TKey; const def: TValue): TValue;
var
  node: PHAMTNode;
  i: Integer;
  h, index: THAMTHash;
  pair: PPair;
begin
  node := @self;
  h := hash(key);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);
    if node.bitmapIsValue.bits[index] then begin
      if node.bitmapIsSinglePointer.bits[index] then
        pair := findInArray( TPairArray(getPointer(index)), key )
      else begin
        pair := node.getPairAddr(index);
        if not equals(pair.key, key) then pair := nil;
      end;
      if pair <> nil then result := pair.value
      else result := def;
      exit
    end else if node.bitmapIsSinglePointer.bits[index] then
      node := PHAMTNode(getPointer(index))
    else
      exit(def);
  end;
  result := def;
end;

end.

