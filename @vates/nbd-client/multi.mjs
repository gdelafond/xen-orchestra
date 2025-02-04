import { asyncEach } from '@vates/async-each'
import { NBD_DEFAULT_BLOCK_SIZE } from './constants.mjs'
import NbdClient from './index.mjs'
import { createLogger } from '@xen-orchestra/log'

const { warn } = createLogger('vates:nbd-client:multi')
export default class MultiNbdClient {
  #clients = []
  #readAhead

  get exportSize() {
    return this.#clients[0].exportSize
  }

  constructor(settings, { nbdConcurrency = 8, readAhead = 16, ...options } = {}) {
    this.#readAhead = readAhead
    if (!Array.isArray(settings)) {
      settings = [settings]
    }
    for (let i = 0; i < nbdConcurrency; i++) {
      this.#clients.push(
        new NbdClient(settings[i % settings.length], { ...options, readAhead: Math.ceil(readAhead / nbdConcurrency) })
      )
    }
  }

  async connect() {
    const connectedClients = []
    for (const clientId in this.#clients) {
      const client = this.#clients[clientId]
      try {
        await client.connect()
        connectedClients.push(client)
      } catch (err) {
        client.disconnect().catch(() => {})
        warn(`can't connect to one nbd client`, { err })
      }
    }
    if (connectedClients.length === 0) {
      throw new Error(`Fail to connect to any Nbd client`)
    }
    if (connectedClients.length < this.#clients.length) {
      warn(
        `incomplete connection by multi Nbd, only ${connectedClients.length} over ${
          this.#clients.length
        } expected clients`
      )
      this.#clients = connectedClients
    }
  }

  async disconnect() {
    await asyncEach(this.#clients, client => client.disconnect(), {
      stopOnError: false,
    })
  }

  async readBlock(index, size = NBD_DEFAULT_BLOCK_SIZE) {
    const clientId = index % this.#clients.length
    return this.#clients[clientId].readBlock(index, size)
  }

  async *readBlocks(indexGenerator) {
    // default : read all blocks
    const readAhead = []
    const makeReadBlockPromise = (index, size) => {
      const promise = this.readBlock(index, size)
      // error is handled during unshift
      promise.catch(() => {})
      return promise
    }

    // read all blocks, but try to keep readAheadMaxLength promise waiting ahead
    for (const { index, size } of indexGenerator()) {
      // stack readAheadMaxLength promises before starting to handle the results
      if (readAhead.length === this.#readAhead) {
        // any error will stop reading blocks
        yield readAhead.shift()
      }

      readAhead.push(makeReadBlockPromise(index, size))
    }
    while (readAhead.length > 0) {
      yield readAhead.shift()
    }
  }
}
