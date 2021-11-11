const ReactiveDao = require("@live-change/dao")
const WebSocket = require('ws')

const debug = false

const opCodes = {
  Ping : 0,
  Pong : 1,
  Ok : 2,
  Error : 3,

  CreateDatabase : 10,
  DeleteDatabase : 11,
  CreateStore : 12,
  DeleteStore : 13,
  OpenStore : 14,
  CloseStore : 15,

  Put : 30,
  Delete : 31,
  DeleteRange: 32,

  Get : 40,
  GetRange : 41,
  GetCount : 42,

  Observe : 50,
  ObserveRange : 51,
  ObserveCount : 52,
  Unobserve : 59,

  Result : 80,
  ResultPut : 81,
  ResultCount : 82,
  ResultNotFound: 83,
  ResultsChanges : 88,
  ResultsDone : 89
}
const opCodeToString = {}
for(const key in opCodes) {
  opCodeToString[opCodes[key]] = key;
}

const rangeFlags = {
  Gt:      0x01,
  Gte:     0x02,
  Lt:      0x04,
  Lte:     0x08,
  Reverse: 0x10,
  Limit:   0x20
}

const resultPutFlags = {
  Found: 0x1,
  Created: 0x2,
  Last: 0x4
}

class DatabaseConnection {
  constructor(url) {
    this.url = url
    this.creatingStores = new Map()
    this.openingStores = new Map()
    this.openStores = []
    this.websocket = null
    this.finished = false
    this.lastRequestId = 0
    this.waitingRequests = new Map()
    this.connect()
  }

  connect() {
    this.websocket = new WebSocket(this.url)
    this.websocket.binaryType = 'nodebuffer'
    this.websocket.on('close', (reason) => {
      console.error("WEBSOCKET CLOSED", reason)
      process.exit(1)
      this.openedStores = []
    })
    this.websocket.on('error', (error) => {
      console.error("WEBSOCKET ERROR", error)
      process.exit(1)
    })
    this.websocket.on('message', (message) => {
      if(typeof message != 'string') { // Buffer
        const opCode = message.readUInt8()
        if(opCode < 2) {
          if(opCode == opCodes.Ping) { // Ping - reply
            message.writeUInt8(opCodes.Pong, 0)
            if(this.websocket.readyState == 1) this.websocket.send(message)
          } else {
            // Pong - ignore
          }
        } else {
          const requestId = message.readUInt32BE(1)
          const request = this.waitingRequests.get(requestId)
          //console.log("RESPONSE TO", requestId, request, "IS", message)
          if(request) {
            if(!request.handlePacket(message)) {
              if(debug) console.log("REQUEST FINISHED", requestId)
              this.waitingRequests.delete(requestId)
            }
          } else {
            if(debug) console.log("unknown response", requestId, message)
          }
        }
      } else { // text
      }
    })
    this.websocket.on('open', () => {
      for(const request of this.waitingRequests.values()) {
        if(request.storeName) {
          this.openStore().then(storeId => {
            request.requestPacket.writeUInt32BE(storeId)
            this.websocket.send(request.requestPacket)
          })
        } else {
          this.websocket.send(request.requestPacket)
        }
      }
    })
    setInterval(() => {
      const packet = Buffer.alloc(1+8)
      packet.writeUInt8(opCodes.Ping, 0)
      packet.writeFloatBE(Date.now(), 1)
      this.websocket.send(packet)
    }, 2000)
  }

  close() {
    this.websocket.close()
    this.finished = true
  }

  addRequest(request) {
    this.waitingRequests.set(request.id, request)
    if(debug) console.log("ADD REQUEST", request.id,"AT STATE", this.websocket.readyState)
    if(this.websocket.readyState == 1) { // 1 => open
      this.websocket.send(request.requestPacket)
    }
  }

  requestOkError(requestPacket) {
    const requestId = ++this.lastRequestId
    requestPacket.writeUInt32BE(requestId, 1)
    return new Promise((resolve, reject) => {
      const request = {
        id: requestId,
        requestPacket,
        handlePacket: packet => {
          const opcode = packet.readInt8(0)
          if(opcode == opCodes.Ok) {
            resolve()
          } else if(opcode == opCodes.Error) {
            const error = packet.toString('utf8', 5)
            reject(error)
          } else {
            console.error("Unknown opcode", opCodeToString[opcode])
            reject("Unknown opcode", opCodeToString[opcode])
          }
          return false // finish waiting
        }
      }
      this.addRequest(request)
    })
  }

  request(requestPacket, handle) {
    const requestId = ++this.lastRequestId
    requestPacket.writeUInt32BE(requestId, 1)
    return new Promise((resolve, reject) => {
      const request = {
        id: requestId,
        requestPacket,
        handlePacket: packet => handle(packet, resolve, reject),
      }
      this.addRequest(request)
    })
  }

  rawRequest(requestPacket, handle) {
    const requestId = ++this.lastRequestId
    requestPacket.writeUInt32BE(requestId, 1)
    const request = {
      id: requestId,
      requestPacket,
      handlePacket: packet => handle(packet),
    }
    this.addRequest(request)
    return requestId
  }

  requestSingleResult(requestPacket) {
    return this.request(requestPacket, (packet, resolve, reject) => {
      //console.log("HANDLE PACKET", packet)
      const opcode = packet.readInt8(0)
      if(opcode == opCodes.Result) {
        resolve(packet.toString('utf8', 5))
      } else if(opcode == opCodes.ResultNotFound) {
        resolve(null)
      } else if(opcode == opCodes.Error) {
        const error = packet.toString('utf8', 5)
        reject(error)
      } else {
        console.error("Unknown opcode", opCodeToString[opcode])
        reject("Unknown opcode", opCodeToString[opcode])
      }
      return false // finish waiting
    })
  }

  async createDatabase(databaseName, settings) {
    const nameBuffer = Buffer.from(databaseName)
    const settingsBuffer = Buffer.from(JSON.stringify(settings));
    const packet = Buffer.alloc(1+4+1+nameBuffer.length+2+settingsBuffer.length)
    packet.writeUInt8(opCodes.CreateDatabase, 0)
    packet.writeUInt8(nameBuffer.length, 1+4)
    nameBuffer.copy(packet, 1+4+1)
    packet.writeUInt16BE(settingsBuffer.length, 1+4+1+nameBuffer.length)
    settingsBuffer.copy(packet, 1+4+1+nameBuffer.length+2)
    if(debug) console.log("SEND CREATE DB", packet)
    return this.requestOkError(packet)
  }

  async deleteDatabase(databaseName) {
    const nameBuffer = Buffer.from(databaseName)
    const packet = Buffer.alloc(1+4+1+nameBuffer.length)
    packet.writeUInt8(opCodes.DeleteDatabase, 0)
    packet.writeUInt8(nameBuffer.length, 1+4)
    nameBuffer.copy(packet, 1+4+1)
    return this.requestOkError(packet)
  }

  async createStore(databaseName, storeName) {
    const storeKey = JSON.stringify([databaseName, storeName])
    const createPromise = this.creatingStores.get(storeKey)
    console.log("CREATING STORE PROMISE", createPromise)
    if(createPromise) return createPromise
    console.log("CREATING STORE", storeKey)
    const databaseNameBuffer = Buffer.from(databaseName)
    const storeNameBuffer = Buffer.from(storeName)
    const packet = Buffer.alloc(1 + 4 + 1 + databaseNameBuffer.length + 1 + storeNameBuffer.length)
    packet.writeUInt8(opCodes.CreateStore, 0)
    packet.writeUInt8(databaseNameBuffer.length, 1 + 4)
    databaseNameBuffer.copy(packet, 1 + 4 + 1)
    packet.writeUInt8(storeNameBuffer.length, 1 + 4 + 1 + databaseNameBuffer.length)
    storeNameBuffer.copy(packet, 1 + 4 + 1 + databaseNameBuffer.length + 1)
    const promise = this.requestOkError(packet)
    this.creatingStores.set(storeKey, promise)
    promise.then(() => {
      this.creatingStores.delete(storeKey)
      console.log("CREATED STORE", storeKey)
    })
    return promise
  }

  async deleteStore(databaseName, storeName) {
    if(debug) console.log("DELETE STORE", databaseName, storeName)
    for(let i = 0; i < this.openStores.length; i++) {
      if(this.openStores[i] && this.openStores[i].storeName == storeName
          && this.openStores[i].databaseName == databaseName) this.openStore[i] = null
    }
    const databaseNameBuffer = Buffer.from(databaseName)
    const storeNameBuffer = Buffer.from(storeName)
    const packet = Buffer.alloc(1+4+1+databaseNameBuffer.length+1+storeNameBuffer.length)
    packet.writeUInt8(opCodes.DeleteStore, 0)
    packet.writeUInt8(databaseNameBuffer.length, 1+4)
    databaseNameBuffer.copy(packet, 1+4+1)
    packet.writeUInt8(storeNameBuffer.length, 1+4+1+databaseNameBuffer.length)
    storeNameBuffer.copy(packet, 1+4+1+databaseNameBuffer.length+1)
    return this.requestOkError(packet)
  }

  async openStore(databaseName, storeName) {
    for(let i = 0; i < this.openStores.length; i++) {
      if(this.openStores[i] && this.openStores[i].storeName == storeName
          && this.openStores[i].databaseName == databaseName) return i
    }
    const storeKey = JSON.stringify([databaseName, storeName])
    console.log("OPEN STORE!", storeKey)
    const createPromise = this.creatingStores.get(storeKey)
    if(createPromise) await createPromise
    const currentPromise = this.openingStores.get(storeKey)
    if(currentPromise) return currentPromise
    console.log("DO OPEN STORE!", storeKey)
    const open = async () => {
      const openStoreId = this.openStores.length
      const databaseNameBuffer = Buffer.from(databaseName)
      const storeNameBuffer = Buffer.from(storeName)
      const packet = Buffer.alloc(1 + 4 + 1 + databaseNameBuffer.length + 1 + storeNameBuffer.length)
      packet.writeUInt8(opCodes.OpenStore, 0)
      packet.writeUInt8(databaseNameBuffer.length, 1 + 4)
      databaseNameBuffer.copy(packet, 1 + 4 + 1)
      packet.writeUInt8(storeNameBuffer.length, 1 + 4 + 1 + databaseNameBuffer.length)
      storeNameBuffer.copy(packet, 1 + 4 + 1 + databaseNameBuffer.length + 1)
      this.openStores.push(null)
      return this.requestOkError(packet).then(ok => {
        this.openStores[openStoreId] = { storeName, databaseName }
        return openStoreId
      }).catch(error => {
        console.error("OPEN STORE FAILED!", error)
        this.openStores[openStoreId] = null
        throw error
      })
    }
    const openPromise = open()
    this.openingStores.set(storeKey, openPromise)
    openPromise.then(() => {
      console.log("STORE OPENED!", storeKey)
      this.openingStores.delete(storeKey)
    })
    return openPromise
  }

  async closeStore(databaseName, storeName) {
    const currentPromise = this.openingStores.get(JSON.stringify(databaseName, storeName))
    let openStoreId = -1
    if(currentPromise) {
      openStoreId = await currentPromise
    } else {
      for(let i = 0; i < this.openStores.length; i++) {
        if(this.openStores[i] && this.openStores[i].storeName == storeName
            && this.openStores[i].databaseName == databaseName) {
          openStoreId = i
          break
        }
      }
    }
    if(openStoreId == -1) return // not opened, ignore
    const packet = Buffer.alloc(1+4+4)
    packet.writeUInt8(opCodes.CloseStore, 0)
    packet.writeUInt32BE(openStoreId, 1+4)
    this.openStores[openStoreId] = null
    return this.requestOkError(packet)
  }

  async put(databaseName, storeName, key, value) {
    const storeId = await this.openStore(databaseName, storeName)
    const keyBuffer = Buffer.from(key)
    const valueBuffer = Buffer.from(value);
    const packet = Buffer.alloc(1+4+4+2+keyBuffer.length+4+valueBuffer.length)
    packet.writeUInt8(opCodes.Put, 0)
    packet.writeUInt32BE(storeId, 1+4)
    packet.writeUInt16BE(keyBuffer.length, 1+4+4)
    keyBuffer.copy(packet, 1+4+4+2)
    if(debug) console.log("PUT", databaseName, "/", storeName , key, "VALUE", value, "BUF")
    packet.writeUInt32BE(valueBuffer.length, 1+4+4+2+keyBuffer.length)
    valueBuffer.copy(packet, 1+4+4+2+keyBuffer.length+4)
    return this.requestSingleResult(packet)
  }

  async delete(databaseName, storeName, key) {
    const storeId = await this.openStore(databaseName, storeName)
    const keyBuffer = Buffer.from(key)
    const packet = Buffer.alloc(1+4+4+2+keyBuffer.length)
    packet.writeUInt8(opCodes.Delete, 0)
    packet.writeUInt32BE(storeId, 1+4)
    packet.writeUInt16BE(keyBuffer.length, 1+4+4)
    keyBuffer.copy(packet, 1+4+4+2)
    return this.requestSingleResult(packet)
  }

  async get(databaseName, storeName, key, value) {
    const storeId = await this.openStore(databaseName, storeName)
    const keyBuffer = Buffer.from(key)
    const packet = Buffer.alloc(1+4+4+2+keyBuffer.length)
    packet.writeUInt8(opCodes.Get, 0)
    packet.writeUInt32BE(storeId, 1+4)
    packet.writeUInt16BE(keyBuffer.length, 1+4+4)
    keyBuffer.copy(packet, 1+4+4+2)
    return this.requestSingleResult(packet)
  }

  rangePacket(storeId, op, range) {
    const min = range.gt || range.gte
    const max = range.lt || range.lte
    const minBuffer = min ? Buffer.from(min) : undefined
    const maxBuffer = max ? Buffer.from(max) : undefined
    const packet = Buffer.alloc(1+4+4+1
        +(min ? 2 + minBuffer.length : 0 )+(max ? 2 + maxBuffer.length : 0 )+(range.limit ? 4 : 0))
    const flags =
        (range.gt && rangeFlags.Gt) | (range.gte && rangeFlags.Gte) |
        (range.lt && rangeFlags.Lt) | (range.lte && rangeFlags.Lte) |
        (range.reverse && rangeFlags.Reverse) |
        (range.limit && rangeFlags.Limit)
    packet.writeUInt8(op, 0)
    packet.writeUInt32BE(storeId, 1+4)
    packet.writeUInt8(flags, 1+4+4)
    let p = 1+4+4+1
    if(min) {
      packet.writeUInt16BE(minBuffer.length, p)
      minBuffer.copy(packet, p + 2)
      p += 2 + minBuffer.length
    }
    if(max) {
      packet.writeUInt16BE(maxBuffer.length, p)
      maxBuffer.copy(packet, p + 2)
      p += 2 + maxBuffer.length
    }
    if(range.limit) {
      packet.writeUInt32BE(range.limit, p)
      p += 4
    }
    return packet
  }

  async getRange(databaseName, storeName, range) {
    const storeId = await this.openStore(databaseName, storeName)
    const packet = this.rangePacket(storeId, opCodes.GetRange, range)
    let results = []
    return this.request(packet, (packet, resolve, reject) => {
      //console.log("HANDLE PACKET", packet)
      const opcode = packet.readInt8(0)
      if(opcode == opCodes.ResultPut) {
        const flags = packet.readUInt8(5) // ignore flags
        const keySize = packet.readUInt16BE(6)
        const key = packet.toString('utf8', 8, 8 + keySize)
        const valueSize = packet.readUInt32BE(8 + keySize)
        const value = packet.toString('utf8', 8 + keySize + 4, 8 + keySize + 4 + valueSize)
        results.push({ key, value })
        return true // need more data
      } else if(opcode == opCodes.ResultsDone) {
        resolve(results)
      } else if(opcode == opCodes.Error) {
        const error = packet.toString('utf8', 5)
        reject(error)
      } else {
        console.error("Unknown opcode", opCodeToString[opcode])
        reject("Unknown opcode", opCodeToString[opcode])
      }
      return false // finish waiting
    })
  }

  async getCount(databaseName, storeName, range) {
    const storeId = await this.openStore(databaseName, storeName)
    const packet = this.rangePacket(storeId, opCodes.GetCount, range)
    return this.request(packet, (packet, resolve, reject) => {
      //console.log("HANDLE PACKET", packet)
      const opcode = packet.readInt8(0)
      if(opcode == opCodes.ResultCount) {
        resolve(packet.readUInt32BE(5))
      } else if(opcode == opCodes.Error) {
        const error = packet.toString('utf8', 5)
        reject(error)
      } else {
        console.error("Unknown opcode", opCodeToString[opcode])
        reject("Unknown opcode", opCodeToString[opcode])
      }
      return false // finish waiting
    })
  }

  async deleteRange(databaseName, storeName, range) {
    const storeId = await this.openStore(databaseName, storeName)
    const packet = this.rangePacket(storeId, opCodes.DeleteRange, range)
    return this.request(packet, (packet, resolve, reject) => {
      //console.log("HANDLE PACKET", packet)
      const opcode = packet.readInt8(0)
      if(opcode == opCodes.ResultCount) {
        resolve(packet.readUInt32BE(5))
      } else if(opcode == opCodes.Error) {
        const error = packet.toString('utf8', 5)
        reject(error)
      } else {
        console.error("Unknown opcode", opCodeToString[opcode])
        reject("Unknown opcode", opCodeToString[opcode])
      }
      return false // finish waiting
    })
  }

  async observe(databaseName, storeName, key, onValue, onError) {
    const storeId = await this.openStore(databaseName, storeName)
    const keyBuffer = Buffer.from(key)
    const packet = Buffer.alloc(1+4+4+2+keyBuffer.length)
    packet.writeUInt8(opCodes.Observe, 0)
    packet.writeUInt32BE(storeId, 1+4)
    packet.writeUInt16BE(keyBuffer.length, 1+4+4)
    keyBuffer.copy(packet, 1+4+4+2)
    if(debug) console.log("OBSERVE KEY", key)
    const requestId = this.rawRequest(packet, (packet) => {
      if(debug) console.log("HANDLE OBSERVE PACKET", packet)
      const opcode = packet.readInt8(0)
      if(opcode == opCodes.Result) {
        return onValue(packet.toString('utf8', 5))
      } else if(opcode == opCodes.ResultNotFound) {
        return onValue(null)
      } else if(opcode == opCodes.Error) {
        const error = packet.toString('utf8', 5)
        onError(error)
        return true
      } else {
        console.error("Unknown opcode", opCodeToString[opcode])
        onError("Unknown opcode: " + opCodeToString[opcode])
      }
      process.exit(1)
      return false// finish waiting
    })
    //console.log("OBSERVE", key, "=>", requestId)
    return requestId
  }

  async observeRange(databaseName, storeName, range, onResultPut, onChanges, onError) {
    const storeId = await this.openStore(databaseName, storeName)
    const packet = this.rangePacket(storeId, opCodes.ObserveRange, range)
    if(debug) console.log("OBSERVE RANGE", databaseName, storeName, range)
    return this.rawRequest(packet, (packet) => {
      if(debug) console.log("OBSERVE RANGE HANDLE PACKET", packet)
      const opcode = packet.readInt8(0)
      if(opcode == opCodes.ResultPut) {
        const flags = packet.readUInt8(5)
        const found = !!(flags & resultPutFlags.Found)
        const last = !!(flags & resultPutFlags.Last)
        const keySize = packet.readUInt16BE(6)
        const key = packet.toString('utf8', 8, 8 + keySize)
        if(found) {
          const valueSize = packet.readInt32BE(8 + keySize)
          const value = packet.toString('utf8', 8 + keySize + 4, 8 + keySize + 4 + valueSize)
          return onResultPut(found, last, key, value)
        } else {
          return onResultPut(found, last, key, null)
        }
      } else if(opcode == opCodes.ResultsChanges) {
        return onChanges()
      } else if(opcode == opCodes.Error) {
        const error = packet.toString('utf8', 5)
        onError(error)
      } else {
        console.error("Unknown opcode", opcode, ":", opCodeToString[opcode])
        onError("Unknown opcode: " + opcode + ':' + opCodeToString[opcode])
      }
      return false// finish waiting
    })
  }

  async observeCount(databaseName, storeName, range, onCount, onError) {
    const storeId = await this.openStore(databaseName, storeName)
    const packet = this.rangePacket(storeId, opCodes.ObserveCount, range)
    return this.rawRequest(packet, (packet) => {
      if(debug) console.log("OBSERVE RANGE HANDLE PACKET", packet)
      const opcode = packet.readInt8(0)
      if(opcode == opCodes.ResultCount) {
        const count = packet.readUInt32BE(5)
        return onCount(count)
      } else if(opcode == opCodes.Error) {
        const error = packet.toString('utf8', 5)
        onError(error)
      } else {
        console.error("Unknown opcode", opcode, ":", opCodeToString[opcode])
        onError("Unknown opcode: " + opcode + ':' + opCodeToString[opcode])
      }
      return false// finish waiting
    })
  }

  async unobserve(requestId) {
    //console.log("UNOBSERVE", requestId)
    if(!this.waitingRequests.get(requestId)) {
      throw new Error("unobserve of not observed! "+requestId)
    }
    if(debug) console.error("REQUEST UNOBSERVED", requestId)
    this.waitingRequests.delete(requestId)
    const packet = Buffer.alloc(1+4)
    packet.writeUInt8(opCodes.Unobserve, 0)
    packet.writeUInt32BE(requestId, 1)
    this.websocket.send(packet)
  }

}



class Store {
  constructor(connection, databaseName, storeName) {
    this.connection = connection
    this.databaseName = databaseName
    this.storeName = storeName

    this.objectObservables = new Map()
    this.rangeObservables = new Map()
    this.countObservables = new Map()
  }

  objectGet(id) {
    if(!id) throw new Error("id is required")
    return this.connection.get(this.databaseName, this.storeName, id)
        .then(result => result && ({ id, ...JSON.parse(result) }))
  }

  objectObservable(id) {
    if(!id) throw new Error("id is required")
    const observableValue = new ReactiveDao.ObservableValue()
    const onValue = (value) => {
      if(debug) console.log("OBJ", id, "ON VALUE", value)
      observableValue.set(value && { id, ...JSON.parse(value) })
      return !observableValue.isDisposed()
    }
    const onError = (error) => {
      console.log("OBJ", id, "ON ERROR", error)
      observableValue.error(error)
      return !observableValue.isDisposed()
    }
    const oldDispose = observableValue.dispose
    const oldRespawn = observableValue.respawn
    observableValue.dispose = () => {
      //console.trace("DISPOSE!")
      //console.log("DISPOSE", observableValue.requestId, this.disposed)
      //if(!this.disposed) this.connection.unobserve(observableValue.requestId)
      oldDispose.apply(this)
    }
    observableValue.respawn = async () => {
      observableValue.requestId = await this.connection.observe(this.databaseName, this.storeName, id,
          onValue, onError)
      oldRespawn.apply(this)
    }
    ;(async () => {
      observableValue.requestId = await this.connection.observe(this.databaseName, this.storeName, id,
          onValue, onError)
    })()
    return observableValue
  }

  async rangeGet(range) {
    if(!range) throw new Error("range not defined")
    const rawResults = await this.connection.getRange(this.databaseName, this.storeName, range)
    return rawResults.map(({ key, value }) => ({ id: key, ...JSON.parse(value) }))
  }

  rangeObservable(range) {
    if(!range) throw new Error("range not defined")
    const observableList = new ReactiveDao.ObservableList()
    let results = []
    let changesMode = false
    const onResultPut = (found, last, key, value) => {
      if(observableList.isDisposed()) {
        console.error("PUT WHEN DISPOSED!", range, found, last, key, value)
        return
      }
      if(changesMode) {
        if(found) {
          if(last) {
            observableList.push({ id: key, ...JSON.parse(value)})
          } else {
            const obj = { id: key, ...JSON.parse(value)}
            observableList.putByField('id', key, obj, range.reverse)
          }
        } else {
          observableList.removeByField('id', key)
        }
      } else if(found) {
        results.push({ id: key, ...JSON.parse(value) })
      } else {
        console.error("non existing object in initial read!")
      }
      return !observableList.isDisposed()
    }
    const onChanges = () => {
      if(observableList.isDisposed()) console.error("CHANGES WHEN DISPOSED!")
      changesMode = true
      observableList.set(results)
      results = []
      return !observableList.isDisposed()
    }
    const oldDispose = observableList.dispose
    const oldRespawn = observableList.respawn
    observableList.dispose = () => {
      this.connection.unobserve(observableList.requestId)
      //console.log("UNOBSERVE STATE", observableList.list)
      //console.trace("UNOBSERVE")
      oldDispose.apply(this)
    }
    observableList.respawn = async () => {
      changesMode = false
      observableList.requestId = await this.connection.observeRange(this.databaseName, this.storeName, range,
          onResultPut, onChanges)
      oldRespawn.apply(this)
    }
    ;(async () => {
      observableList.requestId = await this.connection.observeRange(this.databaseName, this.storeName, range,
          onResultPut, onChanges)
    })()
    return observableList
  }

  countObservable(range) {
    const observableValue = new ReactiveDao.ObservableValue()
    const onValue = (value) => {
      if(debug) console.log("COUNT", range, "ON VALUE", value)
      observableValue.set(value)
      return !observableValue.isDisposed()
    }
    const onError = (error) => {
      if(debug) console.log("COUNT", range, "ON ERROR", error)
      observableValue.error(error)
      return !observableValue.isDisposed()
    }
    const oldDispose = observableValue.dispose
    const oldRespawn = observableValue.respawn
    observableValue.dispose = () => {
      if(observableValue.requestId >= 0) this.connection.unobserve(observableValue.requestId)
      observableValue.requestId = -1
      oldDispose.apply(this)
    }
    observableValue.respawn = async () => {
      observableValue.requestId = await this.connection.observeCount(this.databaseName, this.storeName, range,
          onValue, onError)
      oldRespawn.apply(this)
    }
    ;(async () => {
      observableValue.requestId = await this.connection.observeCount(this.databaseName, this.storeName, range,
          onValue, onError)
    })()
    return observableValue
  }

  async rangeDelete(range) {
    if(!range) throw new Error("range not defined")
    return this.connection.deleteRange(this.databaseName, this.storeName, range)
  }

  async countGet(range) {
    if(!range) throw new Error("range not defined")
    return this.connection.getCount(this.databaseName, this.storeName, range)
  }

  async put(object) {
    const id = object.id
    if(typeof id != 'string') throw new Error(`ID is not string: ${JSON.stringify(id)}`)
    if(debug) console.log("PUT!!!", object)
    if(!id) throw new Error("ID must not be empty string!")
    const putResult = await this.connection.put(this.databaseName, this.storeName, id,
        JSON.stringify({ ...object, id: undefined }))
    return putResult ? { id, ...JSON.parse(putResult) } : null
  }

  async delete(id) {
    if(!id) throw new Error("ID must not be empty string!")
    const deleteResult = await this.connection.delete(this.databaseName, this.storeName, id)
    return deleteResult ? { id, ...JSON.parse(deleteResult) } : null
  }

  async close() {
    return this.connection.closeStore(this.databaseName, this.storeName)
  }
}

Store.Connection = DatabaseConnection

module.exports = Store
