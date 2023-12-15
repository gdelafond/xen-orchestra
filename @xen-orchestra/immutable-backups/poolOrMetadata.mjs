import { join } from 'node:path'
import * as Directory from './file.mjs'
import {  watchForNew } from './newFileWatcher.mjs'

async function waitForCompletion(path) {
  await watchForNew(path, (pathInDirectory, isNew, watcher) => {
    if (pathInDirectory === 'metadata.json') {
      watcher.close()
      // will end the watcher, stop this loop and return
    }
  })
}

export async function watch(basePath, immutabilityCachePath) {
  await watchForNew(basePath, async pathInDirectory => {
    const path = join(basePath, pathInDirectory)
    await waitForCompletion(path)
    await Directory.makeImmutable(path, immutabilityCachePath)
  })
}
