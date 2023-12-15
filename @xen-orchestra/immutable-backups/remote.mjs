import fs from 'node:fs/promises'
import { watch as watchPoolOrMetadata } from './poolOrMetadata.mjs'
import * as Vm from './vm.mjs'
import * as File from './file.mjs'
import path from 'node:path'
import { createLogger } from '@xen-orchestra/log'
import { watchForExistingAndNew } from './newFileWatcher.mjs'

const { warn } = createLogger('xen-orchestra:immutable-backups:remote')

async function test(remotePath) { 
  await fs.readdir(remotePath)
const immutabilityCache = path.join(remotePath, '.immutable')
try{

    await fs.mkdir(immutabilityCache)
}catch(err){
    if(err.code !== 'EEXIST'){
        throw err
    }
}
  const testPath = path.join(remotePath, '.test-immut')
  // cleanup
  try {
    await File.liftImmutability(testPath,immutabilityCache)
    await fs.unlink(testPath)
  } catch (err) {}
  // can create , modify and delete a file
  await fs.writeFile(testPath, `test immut ${new Date()}`)
  await fs.writeFile(testPath, `test immut change 1 ${new Date()}`)
  await fs.unlink(testPath)
  
  // cannot modify or delete an immutable file
  await fs.writeFile(testPath, `test immut ${new Date()}`)
  await File.makeImmutable(testPath,immutabilityCache)
  try {
    await fs.writeFile(testPath, `test immut change 2  ${new Date()}`)
    await fs.unlink(testPath)
    throw new Error(`breach of contract : succeed in modifying and deleteing an immutable file`)
  } catch (error) {
    if (error.code !== 'EPERM') {
      throw error
    }
  }
  // can modify and delete a file after lifting immutability
  await File.liftImmutability(testPath,immutabilityCache)
  await fs.writeFile(testPath, `test immut change 3 ${new Date()}`)
  await fs.unlink(testPath)
}

async function liftImmutability(remoteRootPath, immutabilityDuration) {
  throw new Error('to reimplement')
}

export async function watchRemote(remoteRootPath, immutabilityDuration) {
  await test(remoteRootPath)

  // add duration and watch status in the metadata.json of the remote
  await fs.writeFile(
    path.join(
      remoteRootPath,
      '.immutable-settings.json'
    ),
      JSON.stringify({
        since: +new Date(),
        immutable: true,
        duration: immutabilityDuration,
      })
    
  )

const immutabilityCachePath = path.join(remoteRootPath, '.immutable')
  // watch the remote for any new VM metadata json file

  watchForExistingAndNew(remoteRootPath, async pathInRemote => {
    if (pathInRemote === 'xo-vm-backups') {
      // watch all the VMs existing and yet to come
      watchForExistingAndNew(path.join(remoteRootPath, 'xo-vm-backups'), async vmId => {
        if (vmId.endsWith('.lock')) {
          console.log(vmId, ' is lock dir, skip')
          return
        }
        Vm.watch(path.join(remoteRootPath, 'xo-vm-backups', vmId),immutabilityCachePath)
      }).catch(warn)
    }

    if (['xo-config-backups', 'xo-pool-metadata-backups'].includes(pathInRemote)) {
      watchPoolOrMetadata(path.join(remoteRootPath, 'xo-vm-backups'),immutabilityCachePath).catch(warn)
    }
  }).catch(warn)

  // @todo : shoulw also watch metadata and pool backups
}

async function deepCheck() {
  // ensure all the files linked to a backup are in line with the json immutability status
  // check if any immutable file has been modified
}
/**
 * This try to attains the "governance mode" of object locking
 *
 * What does it protect you against ?
 *  deletion by user before the end of retention period
 *
 *
 * What does it left unprotected
 *
 *  an user modifying the files during upload
 *  an user modifying the files after a disk upload , but before the full upload of the backup
 *  bit rot
 * ====> theses 3 are mitigated by authenticated encryption. Modification will be detected on restore and fail.
 *
 *  an user with root access to your FS can lift immutability , and access the remote settings containing
 *  the encryption key
 *
 */

watchRemote('/mnt/ssd/vhdblock/', 7 * 24 * 60 * 60 * 1000)
