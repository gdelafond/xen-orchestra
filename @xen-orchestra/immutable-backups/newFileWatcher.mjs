import { asyncEach } from '@vates/async-each'
import fs from 'node:fs/promises'

export async function watchForNew(path, callback){
    const watcher = fs.watch(path)
    for await (const { eventType, filename } of watcher) {
      if( filename === null ){
        console.log(`${filename} is null `)
          // ok , keep your secrets for yourself
          continue
      }
      if (filename.startsWith('.')) {
        // temp file during upload
        continue
      } 
      if(eventType ==='change'){
        // continue
      }
      try{
        await fs.stat(filename)
      }catch(err){
        if(err.code !== 'ENOENT'){
          throw err
        }
        console.log('file does not exists', filename)
      }
      await callback(filename, true, watcher)
    }
}

export async function watchForExistingAndNew(path, callback) {
  await asyncEach(await fs.readdir(path)
    , entry => callback(entry))

  await watchForNew(path, callback)
}
