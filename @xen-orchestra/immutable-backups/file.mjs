import execa from 'execa'
import fs from 'node:fs/promises'
import { computeCacheFilePath } from './computeCacheFilePath.mjs'

// this work only on linux like systems
// this could wokr on windwos : https://4sysops.com/archives/set-and-remove-the-read-only-file-attribute-with-powershell/

export async function makeImmutable(path, immutabilityCachePath) {
  const cacheFileName = computeCacheFilePath(path, immutabilityCachePath, true)
  await fs.writeFile(cacheFileName, path)
  await execa('chattr', ['+i', path])
  await execa('chattr', ['+i', cacheFileName])
}

export async function liftImmutability(filePath, immutabilityCachePath) {
    console.log('lift file', filePath, immutabilityCachePath)

    const cacheFileName = computeCacheFilePath(filePath, immutabilityCachePath, true)
    try{ 
  await execa('chattr', ['-i', cacheFileName])
}catch(err){
    console.log('error while clearing cache immut', err)
}
console.log('cleared immut cache')
  await execa('chattr', ['-i', filePath])
  console.log('cleared file')
  await fs.unlink(cacheFileName)
console.log('cleared cache')

}
