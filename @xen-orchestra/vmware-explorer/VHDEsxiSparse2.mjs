import _computeGeometryForSize from 'vhd-lib/_computeGeometryForSize.js'
import { createFooter, createHeader } from 'vhd-lib/_createFooterHeader.js'
import { DISK_TYPES, FOOTER_SIZE } from 'vhd-lib/_constants.js'
import { notEqual, strictEqual } from 'node:assert'
import { unpackFooter, unpackHeader } from 'vhd-lib/Vhd/_utils.js'
import { VhdAbstract } from 'vhd-lib'


// one big difference with the other versions of VMDK is that the grain tables are actually sparse, they are pre-allocated but not used in grain order,
// so we have to read the grain directory to know where to find the grain tables

const SE_SPARSE_DIR_NON_ALLOCATED = 0
const SE_SPARSE_DIR_ALLOCATED = 1

const SE_SPARSE_GRAIN_NON_ALLOCATED = 0 // check in parent
const SE_SPARSE_GRAIN_UNMAPPED = 1 // grain has been unmapped, but index of previous grain still readable for reclamation
const SE_SPARSE_GRAIN_ZERO = 2
const SE_SPARSE_GRAIN_ALLOCATED = 3

const VHD_BLOCK_SIZE_BYTES = 2 * 1024* 1024 

const ones = n => (1n << BigInt(n)) - 1n

function asNumber (n) {
  if (n > Number.MAX_SAFE_INTEGER)
    throw new Error(`can't handle ${n} ${Number.MAX_SAFE_INTEGER} ${n & 0x00000000ffffffffn}`)
  return Number(n)
}

const readInt64 = (buffer, index) => asNumber(buffer.readBigInt64LE(index * 8))

/**
 * @returns {{topNibble: number, low60: bigint}} topNibble is the first 4 bits of the 64 bits entry, indexPart is the remaining 60 bits
 */
function readTaggedEntry (buffer, index) {
  const entry = buffer.readBigInt64LE(index * 8)
  return {topNibble: Number(entry >> 60n), low60: entry & ones(60)}
}

function readSeSparseDir (buffer, index) {
  const {topNibble, low60} = readTaggedEntry(buffer, index)
  return {type: topNibble, tableIndex: asNumber(low60)}
}

function readSeSparseTable (buffer, index) {
  const {topNibble, low60} = readTaggedEntry(buffer, index)
  // https://lists.gnu.org/archive/html/qemu-block/2019-06/msg00934.html
  const topIndexPart = low60 >> 48n // bring the top 12 bits down
  const bottomIndexPart = (low60 & ones(48)) << 12n // bring the bottom 48 bits up
  return {type: topNibble, grainIndex: asNumber(bottomIndexPart | topIndexPart)}
}


export default class VHDEsxiSparse2 extends VhdAbstract {
  #esxi
  #datastore
  #parentVhd
  #path
  #lookMissingBlockInParent

  #header
  #footer

  #grainIndex // Map blockId => []

  static async open(esxi, datastore, path, parentVhd, opts) {
    const vhd = new VHDEsxiSparse2(esxi, datastore, path, parentVhd, opts)
    await vhd.readHeaderAndFooter()
    return vhd
  }

  get path(){
    return this.#path
  }
  constructor(esxi, datastore, path, parentVhd, { lookMissingBlockInParent = true } = {}) {
    super()
    this.#esxi = esxi
    this.#path = path
    this.#datastore = datastore
    this.#parentVhd = parentVhd
    this.#lookMissingBlockInParent = lookMissingBlockInParent
  }

  get header() {
    return this.#header
  }

  get footer() {
    return this.#footer
  }

  containsBlock(blockId) {
    notEqual(this.#grainIndex, undefined, "bat must be loaded to use contain blocks'")
    return (
      this.#grainIndex.get(blockId ) !==  undefined||
      (this.#lookMissingBlockInParent && this.#parentVhd.containsBlock(blockId))
    )
  }

  async #read(start, length) {
    return (await this.#esxi.download(this.#datastore, this.#path, `${start}-${start + length - 1}`)).buffer()
  }

  async readHeaderAndFooter() {
    const vmdkHeaderBuffer = await this.#read(0, 2048)

    strictEqual(vmdkHeaderBuffer.readBigInt64LE(0), 0xcafebaben)
    strictEqual(readInt64(vmdkHeaderBuffer, 1), 0x200000001) // version 2.1

    this.grainDirOffsetBytes = readInt64(vmdkHeaderBuffer, 16) * 512
    console.log('grainDirOffsetBytes', this.grainDirOffsetBytes)
    this.grainDirSizeBytes = readInt64(vmdkHeaderBuffer, 17) * 512
    console.log('grainDirSizeBytes', this.grainDirSizeBytes) 
    this.grainDirCount = this.grainDirSizeBytes / 8  // count is the number of 64b entries in the array

    this.grainSizeSectors = readInt64(vmdkHeaderBuffer, 3)
    this.grainSizeBytes = this.grainSizeSectors * 512 // 8 sectors = 4KB default
    strictEqual(this.grainSizeBytes, 4096)
    console.log('grainSizeBytes', this.grainSizeBytes)
    this.grainTableOffsetBytes = readInt64(vmdkHeaderBuffer, 18) * 512
    console.log('grainTableOffsetBytes', this.grainTableOffsetBytes)

    this.grainTableCount = readInt64(vmdkHeaderBuffer, 4) * 512 / 8// count is the number of 64b entries in each tables
    console.log('grainTableCount', this.grainTableCount)

    this.grainOffsetBytes = readInt64(vmdkHeaderBuffer, 24) * 512
    console.log('grainOffsetBytes', this.grainOffsetBytes)

    const sizeBytes = readInt64(vmdkHeaderBuffer, 2) * 512
    console.log('sizeBytes', sizeBytes)

    this.#header = unpackHeader(createHeader(Math.ceil(sizeBytes / VHD_BLOCK_SIZE_BYTES)))
    const geometry = _computeGeometryForSize(sizeBytes)
    const actualSize = geometry.actualSize
    this.#footer = unpackFooter(
      createFooter(actualSize, Math.floor(Date.now() / 1000), geometry, FOOTER_SIZE, DISK_TYPES.DYNAMIC)
    )
  }

  async readBlockAllocationTable() {
    
    this.#grainIndex = new Map()

    const grainDirBuffer = await this.#read(this.grainDirOffsetBytes, this.grainDirOffsetBytes +  this.grainDirSizeBytes)
    // read the grain dir ( first level )
    for(let grainDirIndex=0 ; grainDirIndex < this.grainDirCount ; grainDirIndex++){
        const {type:grainDirType, tableIndex} = readSeSparseDir(grainDirBuffer, grainDirIndex)
        if(grainDirType ===SE_SPARSE_DIR_NON_ALLOCATED){
          // no grain table allocated at all in this grain dir
          continue
        }
        strictEqual(grainDirType, SE_SPARSE_DIR_ALLOCATED)
        // read the corresponding grain table ( second level )
        const grainTableBuffer = await this.#read(this.grainTableOffsetBytes + tableIndex * this.grainTableCount*8,this.grainTableCount*8)
        let grains = []
        let blockId = grainDirIndex * 8

        const  addGrain = (val) => {
            grains.push(val)
            // 4096 block of 4Kb per dir entry =>16MB/grain dir
            // 1 block = 2MB
            // 512 grain => 1 block
            // 8 block per dir entry
            if(grains.length === 512){ 
                this.#grainIndex.set(blockId, grains)
                grains = []
                blockId ++
            }
        }

        for(let grainTableIndex =0; grainTableIndex < grainTableBuffer.length / 8; grainTableIndex ++){
          const {type:grainType,grainIndex } = readSeSparseTable(grainTableBuffer,grainTableIndex)
          if(grainType === SE_SPARSE_GRAIN_ALLOCATED ){
            // this is ok in 32 bits int with VMDK smaller than 2TB
            const offsetByte = grainIndex*this.grainSizeBytes + this.grainOffsetBytes 
            addGrain(offsetByte) 
          } else {
            // multiply by -1 to differenciate type and offset
            // no offset can be zero 
            addGrain(grainType  * -1 )
          }
        } 
        strictEqual(grains.length, 0)
    }
  }

  async readBlock(blockId) {
    let changed = false
    const parentBlock = await this.#parentVhd.readBlock(blockId)
    const parentBuffer = parentBlock.buffer
    const grainOffsets = this.#grainIndex.get(blockId) // may be undefined if the child contains block and lookMissingBlockInParent=true
    const EMPTY_GRAIN =   Buffer.alloc(this.grainSizeBytes, 0)
    for(const index in grainOffsets){
        const value = grainOffsets[index]
        let data
        if(value >0){
          // it's the offset in byte of a grain type SE_SPARSE_GRAIN_ALLOCATED
            data = await this.#read(value,this.grainSizeBytes)
        } else {
          // back to the real grain type
          const type = value*-1
          switch(type){
            case SE_SPARSE_GRAIN_ZERO:
            case SE_SPARSE_GRAIN_UNMAPPED:
              data = EMPTY_GRAIN
              break;
            case SE_SPARSE_GRAIN_NON_ALLOCATED:
              /* from parent */
              break;
            default:
              throw new Error(`can't handle grain type ${type}`)
          }
        }
        if(data){
          changed = true
          data.copy(parentBuffer, (index *this.grainSizeBytes)  + 512 /* block bitmap */)
        }
    }
    // no need to copy if data all come from parent
    return changed ? {
      id: blockId,
      bitmap: parentBuffer.slice(0, 512),
      data: parentBuffer.slice(512),
      buffer: parentBuffer,
    } : parentBlock
  }
}