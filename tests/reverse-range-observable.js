const test = require('tape')

const Store = require('../lib/Store.js')
const { prepareDatabaseTest } = require('./utils.js')

const serverUrl = "ws://localhost:3530/api/ws"
const dbName = "test"
const storeName = "test"
const connection = new Store.Connection(serverUrl)

test("store range observable", t => {
  t.plan(6)

  let store

  prepareDatabaseTest(t, connection, dbName, storeName)

  t.test("create store", async t => {
    t.plan(1)
    store = new Store(connection, dbName, storeName)
    t.pass('store created')
  })

  t.test("put objects", async t => {
    t.plan(1)
    await store.put({ v: 1, id: 'a' })
    await store.put({ v: 3, id: 'c' })
    t.pass('objects written')
  })

  let nextValueResolve
  let gotNextValue
  const getNextValue = () => {
    if(gotNextValue) {
      gotNextValue = false
      return rangeObservable.list
    }
    return new Promise((resolve, reject) => nextValueResolve = resolve)
  }
  let rangeObservable
  const rangeObserver = (signal, value, ...rest) => {
    console.log("SIGNAL", signal, value, ...rest)
    gotNextValue = true
    if(nextValueResolve) nextValueResolve(rangeObservable.list)
  }

  t.test("observe reverse range [z,a]", async t => {
    t.plan(6)
    rangeObservable = store.rangeObservable({ gte: 'a', lte: 'z', reverse: true })
    rangeObservable.observe(rangeObserver)
    let values = await getNextValue()
    t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 1, id: 'a' } ], 'range value' )

    t.test("remove object 'a' from observed range", async t => {
      t.plan(1)
      await store.delete('a')
      let values = await getNextValue()
      t.deepEqual(values, [ { v: 3, id: 'c' } ], 'range value' )
    })

    t.test("add object 'a' to observed range", async t => {
      t.plan(1)
      await store.put({ id: 'a', v: 4 })
      let values = await getNextValue()
      t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 4, id: 'a' } ], 'range value' )
    })

    t.test("add object 'b' to observed range", async t => {
      t.plan(1)
      await store.put({ id: 'b', v: 5 })
      let values = await getNextValue()
      t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 5, id: 'b' }, { v: 4, id: 'a' } ], 'range value' )
    })

    t.test("add object 'd' to observed range", async t => {
      t.plan(1)
      await store.put({ id: 'd', v: 6 })
      let values = await getNextValue()
      t.deepEqual(values, [ { v: 6, id: 'd' }, { v: 3, id: 'c' }, { v: 5, id: 'b' }, { v: 4, id: 'a' } ], 'range value' )
    })

    t.test("unobserve range", async t => {
      t.plan(1)
      rangeObservable.unobserve(rangeObserver)
      t.pass('unobserved')
    })
  })

  t.test("observe reverse range (d, a)", async t => {
    t.plan(6)
    rangeObservable = store.rangeObservable({ gt: 'a', lt: 'd', reverse: true })
    rangeObservable.observe(rangeObserver)
    let values = await getNextValue()
    t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 5, id: 'b' } ], 'range value' )

    t.test("remove object 'd' outside observed range", async t => {
      t.plan(1)
      await store.delete('d')
      t.pass('deleted')
    })

    t.test("remove object 'a' outside observed range", async t => {
      t.plan(1)
      await store.delete('a')
      t.pass('deleted')
    })

    t.test("add object 'ab' to observed range", async t => {
      t.plan(1)
      await store.put({ id: 'ab', v: 7 })
      let values = await getNextValue()
      t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 5, id: 'b' }, { v: 7, id: 'ab' } ], 'range value' )
    })

    t.test("remove object 'ab' from observed range", async t => {
      t.plan(1)
      await store.delete('ab')
      let values = await getNextValue()
      t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 5, id: 'b' } ], 'range value' )
    })

    t.test("unobserve range", async t => {
      t.plan(1)
      rangeObservable.unobserve(rangeObserver)
      t.pass('unobserved')
    })
  })

  t.test("close and remove database", async t => {
    t.plan(1)
    await connection.deleteDatabase(dbName)
    await connection.close()
    t.pass('closed')
    t.end()
  })
})
