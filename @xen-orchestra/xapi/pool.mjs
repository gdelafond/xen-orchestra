import { asyncEach } from '@vates/async-each'
import { createLogger } from '@xen-orchestra/log'

import { getCurrentVmUuid } from './_XenStore.mjs'

const noop = Function.prototype

async function pCatch(p, code, cb) {
  try {
    return await p
  } catch (error) {
    if (error.code === code) {
      return cb(error)
    }
    throw error
  }
}

const { warn } = createLogger('xo:xapi:pool')

export default class Pool {
  async emergencyShutdown() {
    const poolMasterRef = this.pool.master

    let currentVmRef
    try {
      currentVmRef = await this.call('VM.get_by_uuid', await getCurrentVmUuid())

      // try to move current VM on pool master
      const hostRef = await this.call('VM.get_resident_on', currentVmRef)
      if (hostRef !== poolMasterRef) {
        await this.callAsync('VM.pool_migrate', currentVmRef, poolMasterRef, {})
      }
    } catch (error) {
      warn(error)
    }

    await pCatch(this.call('pool.disable_ha'), 'HA_NOT_ENABLED', noop).catch(warn)

    const hostRefs = await this.call('host.get_all')
    await asyncEach(hostRefs, async hostRef => {
      await this.call('host.disable', hostRef).catch(warn)

      const vmRefs = await this.call('host.get_resident_VMs', hostRef)
      await asyncEach(vmRefs, vmRef => {
        // never stop current VM otherwise the emergencyShutdown process would be interrupted
        if (vmRef === currentVmRef) {
          return pCatch(this.callAsync('VM.suspend', vmRef), 'VM_BAD_POWER_STATE', noop).catch(warn)
        }
      })

      // pool master will be shutdown at the end
      if (hostRef !== poolMasterRef) {
        await this.callAsync('host.shutdown', hostRef).catch(warn)
      }
    })

    await this.callAsync('host.shutdown', poolMasterRef)
  }
}
