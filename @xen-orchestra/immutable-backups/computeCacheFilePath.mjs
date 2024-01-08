import { extname, join } from 'node:path'
import { sha256 } from './directory.mjs' 

export function computeCacheFilePath(path, immutabilityCachePath, isFile) {
  const ext = isFile ? `file${extname(path)}` : 'dir'
  return join(immutabilityCachePath, 
    `${sha256(path)}.${ext}`)
}
  