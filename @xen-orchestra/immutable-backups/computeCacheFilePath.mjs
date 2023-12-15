import { basename, join } from 'node:path'
import { sha256 } from './directory.mjs' 

export function computeCacheFilePath(path, immutabilityCachePath, isFile) {
  console.log({path, immutabilityCachePath, isFile})
  return join(immutabilityCachePath, `${sha256(path)}.${isFile ? 'file' : 'dir'}.${basename(path)}`)
}
  