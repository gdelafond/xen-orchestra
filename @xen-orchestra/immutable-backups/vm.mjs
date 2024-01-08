import fs from 'node:fs/promises'

import * as File from './file.mjs'
import * as Directory from './directory.mjs'
import { createLogger } from '@xen-orchestra/log'
import { extname, join } from 'node:path'
import { watchForNew, watchForExistingAndNew } from './newFileWatcher.mjs'

const { warn } = createLogger('xen-orchestra:immutable-backups:vm')

const WATCHED_FILE_EXTENSION = ['.json', '.xva', '.checksum']
// xo-vm-backups/<vm uuid>/vdis/<job uuid>/<vdi uuid>/<uuid>.vhd
// xo-vm-backups/<vm uuid>/vdis/<job uuid>/<vdi uuid>/data/<uuid.vhd>/bat|footer|header|blocks
async function waitForVhdDirectoryCompletion(vhdDirectoryPath) {
    console.log('waitForVhdDirectoryCompletion', vhdDirectoryPath)
  const metadataStatus = {
    bat: false,
    footer: false,
    header: false,
    "chunk-filters.json" : false
  }
  const METADATA_FILES = ['bat', 'footer', 'header', 'chunk-filters.json']
  await watchForExistingAndNew(vhdDirectoryPath, (pathInDirectory, _, watcher) => {
    if (METADATA_FILES.includes(pathInDirectory)) {
      console.log({pathInDirectory})
      metadataStatus[pathInDirectory] = true
      if (Object.values(metadataStatus).every(t => t)) {
        console.log('>>>vhd is complete' ,watcher)
        watcher.close()
        // will end the watcher, stop this loop and return
      } else{
        console.log('not complete', metadataStatus)
      }
    } else{
      console.log('not watched' , pathInDirectory)
    }
  })
}

async function watchVdiData(dataPath, immutabilityCachePath) {
  await watchForNew(dataPath, async pathInDirectory => {
    const path = join(dataPath, pathInDirectory) 
      // @todo : add timeout
      await waitForVhdDirectoryCompletion(path, immutabilityCachePath)
      console.log('vhd is complete, will make immut')
      await Directory.makeImmutable(path, immutabilityCachePath) 
  })
}

async function watchVdi(vdiPath, immutabilityCachePath) {
  await watchForExistingAndNew(vdiPath, async (pathInDirectory, isNew) => {
    const path = join(vdiPath, pathInDirectory)
    if (pathInDirectory === 'data') {
      // vhd directory mode
      watchVdiData(path, immutabilityCachePath).catch(warn)
    } else {
      // alias or real vhd file
      if (isNew === true) {
        await File.makeImmutable(path, immutabilityCachePath)
      }
    }
  })
}

async function watchJob(jobPath, immutabilityCachePath) {
  await watchForExistingAndNew(jobPath, async jobId => {
    const vdiPath = join(jobPath, jobId)
    const stats = await fs.stat(vdiPath)
    if (stats.isDirectory()) {
      watchVdi(vdiPath, immutabilityCachePath).catch(warn)
    }
  })
}

async function watchVdis(vdisPath, immutabilityCachePath) {
  await watchForExistingAndNew(vdisPath, async jobId => {
    const path = join(vdisPath, jobId)
    const stats = await fs.stat(path)
    if (stats.isDirectory()) {
      watchJob(path, immutabilityCachePath).catch(warn)
    }
  })
}

export async function watch(vmPath, immutabilityCachePath) {
  await watchForExistingAndNew(vmPath, async (pathInDirectory, isNew) => {
    const path = join(vmPath, pathInDirectory)
    if (pathInDirectory === 'vdis') {
      watchVdis(path, immutabilityCachePath).catch(warn)
    } else {
      if (isNew === true && WATCHED_FILE_EXTENSION.includes(extname(pathInDirectory))) {
        await File.makeImmutable(path, immutabilityCachePath)
      }
    }
  })
}
