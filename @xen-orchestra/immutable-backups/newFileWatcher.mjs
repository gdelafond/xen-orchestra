import { asyncEach } from '@vates/async-each'
import fs from 'node:fs/promises'

export async function watchForNew(path, callback){
    console.log('WATCH FOR NEW IN ', path )
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
      try{
          const stat = await fs.stat(path)
          if (stat.mtimeMs === stat.ctimeMs) {
            await callback(filename, true, watcher)
          } else {
            console.log('not new ', {stat})
          }
      }catch(error){
  console.warn(error)
      }
    }
}

export async function watchForExistingAndNew(path, callback) {
console.log('WATCH FOR ', path )
  await asyncEach(await fs.readdir(path)
    , entry => callback(entry))

  await watchForNew(path, callback)
}
