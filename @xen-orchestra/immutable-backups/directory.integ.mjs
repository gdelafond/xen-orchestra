import {describe, it} from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import path from 'node:path'
import { tmpdir } from 'node:os';
import * as Directory from './directory.mjs'
import { rimraf } from 'rimraf'


describe('immutable-backups/file', async()=>{

    it('really lock a directory', async()=>{
        const dir = await fs.mkdtemp(path.join(tmpdir(),'immutable-backups-tests'))
        const immutDir = path.join(tmpdir(),'.immutable')
        await fs.mkdir(immutDir)
        const filePath = path.join( dir, 'test')
        await fs.writeFile(filePath, 'data')
        await Directory.makeImmutable(dir,immutDir)
        await assert.rejects(()=>fs.writeFile(filePath, 'data'))
        await assert.rejects(()=>fs.appendFile(filePath, 'data'))
        await assert.rejects(()=>fs.unlink(filePath))
        await assert.rejects(()=>fs.rename(filePath, filePath+'copy'))
        await assert.rejects(()=>fs.writeFile(path.join(dir, 'test2'), 'data'))
        await assert.rejects(()=>fs.rename(dir, dir+'copy'))
        await Directory.liftImmutability(dir,immutDir)
        await fs.writeFile(filePath, 'data')
        await fs.appendFile(filePath, 'data')
        await fs.unlink(filePath)
        await fs.rename(dir, dir+'copy')
        await rimraf(dir)
    })
})