const test = require('tape')

const Store = require('../lib/Store.js')
const { prepareDatabaseTest } = require('./utils.js')

const serverUrl = "ws://localhost:3530/api/ws"
const dbName = "test"
const storeName = "test"
const connection = new Store.Connection(serverUrl)


test("store non-reactive properties", t => {
  t.plan(4)

  let store

  prepareDatabaseTest(t, connection, dbName, storeName)

  t.test("create store", async t => {
    t.plan(1)
    store = new Store(connection, dbName, storeName)
    t.pass('store created')
  })

  t.test("non reactive operations", async t => {
    t.plan(25)

    t.test("put value", async t => {
      t.plan(1)
      try {
        await store.put({ id:'a', v: 1 })
        t.pass('value written')
      } catch(e) {
        t.fail(e)
      }
    })

    t.test("get value", async t => {
      t.plan(1)
      try {
        const v = await store.objectGet('a')
        t.deepEqual(v, { v: 1, id: 'a' }, 'value read')
      } catch(e) {
        t.fail(e)
      }
    })

    t.test("get non existing value", async t => {
      t.plan(1)
      try {
        const v = await store.objectGet('z')
        t.deepEqual(v, null, 'value read')
      } catch(e) {
        t.fail(e)
      }
    })

    t.test("put another values", async t => {
      t.plan(1)
      try {
        await store.put({ id: 'c', v: 3 })
        await store.put({ id: 'b', v: 2 })
        t.pass('values written')
      } catch(e) {
        t.fail(e)
      }
    })

    t.test("get range [a,c]", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: '0', lte: 'c' })
      t.deepEqual(values, [ { v: 1, id: 'a' }, { v: 2, id: 'b' }, { v: 3, id: 'c' } ], 'range read' )
    })

    t.test("count range [a,c]", async t => {
      t.plan(1)
      let values = await store.countGet({ gte: 'a', lte: 'c' })
      t.deepEqual(values, 3, 'range count' )
    })

    t.test("count all", async t => {
      t.plan(1)
      let values = await store.countGet({ })
      t.deepEqual(values, 3, 'full count' )
    })


    t.test("get reverse range [c,a]", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lte: 'c', reverse: true })
      t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 2, id: 'b' },  { v: 1, id: 'a' } ], 'range read' )
    })

    t.test("count reverse range [a,c]", async t => {
      t.plan(1)
      let values = await store.countGet({ gte: 'a', lte: 'c', reverse: true })
      t.deepEqual(values, 3, 'range count' )
    })

    t.test("get range [a,c] with limit 2", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lte: 'c', limit: 2 })
      t.deepEqual(values, [ { v: 1, id: 'a' }, { v: 2, id: 'b' } ], 'range read' )
    })

    t.test("count range [a,c] with limit 2", async t => {
      t.plan(1)
      let values = await store.countGet({ gte: 'a', lte: 'c', limit: 2 })
      t.deepEqual(values, 2, 'range read' )
    })

    t.test("get reverse range [c,a] with limit 2", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lte: 'c', reverse: true, limit: 2 })
      t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 2, id: 'b' } ], 'range read' )
    })

    t.test("get reverse count [c,a] with limit 2", async t => {
      t.plan(1)
      let values = await store.countGet({ gte: 'a', lte: 'c', reverse: true, limit: 2 })
      t.deepEqual(values, 2, 'range read' )
    })

    t.test("get range (a,c]", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gt: 'a', lte: 'c' })
      t.deepEqual(values, [ { v: 2, id: 'b' }, { v: 3, id: 'c' } ], 'range read' )
    })

    t.test("get reverse range [c,a)", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gt: 'a', lte: 'c', reverse: true })
      t.deepEqual(values, [ { v: 3, id: 'c' }, { v: 2, id: 'b' }, ], 'range read' )
    })

    t.test("get range [a,c)", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lt: 'c' })
      t.deepEqual(values, [ { v: 1, id: 'a' }, { v: 2, id: 'b' } ], 'range read' )
    })

    t.test("get reverse range (c,a]", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lt: 'c', reverse: true })
      t.deepEqual(values, [ { v: 2, id: 'b' }, { v: 1, id: 'a' } ], 'range read' )
    })

    t.test("get range (a,c)", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gt: 'a', lt: 'c' })
      t.deepEqual(values, [ { v: 2, id: 'b' } ], 'range read' )
    })

    t.test("get reverse range (a,c)", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gt: 'a', lt: 'c', reverse: true })
      t.deepEqual(values, [ { v: 2, id: 'b' } ], 'range read' )
    })

    t.test("remove 'b'", async t => {
      t.plan(1)
      try {
        await store.delete('b')
        t.pass("removed")
      } catch(e) {
        t.fail(e)
      }
    })

    t.test("get range [a,c]", async t => {
      t.plan(1)
      let values = await store.rangeGet({ gte: 'a', lte: 'c' })
      t.deepEqual(values, [ { v: 1, id: 'a' }, { v: 3, id: 'c' } ], 'range read' )
    })

    t.test("put another values", async t => {
      t.plan(1)
      try {
        await store.put({ id: 'd', v: 4 })
        await store.put({ id: 'e', v: 5 })
        t.pass('values written')
      } catch(e) {
        t.fail(e)
      }
    })

    t.test("get range all", async t => {
      t.plan(1)
      let values = await store.rangeGet({ })
      t.deepEqual(values, [ { v: 1, id: 'a' }, { v: 3, id: 'c' }, { id: 'd', v: 4 }, { id: 'e', v: 5 } ], 'range read' )
    })

    t.test("remove range ['c','d']", async t => {
      t.plan(1)
      try {
        await store.rangeDelete({ gte: 'c', lte: 'd' })
      } catch(e) {
        t.fail(e)
      }
      t.pass("removed")
    })

    t.test("get range all", async t => {
      t.plan(1)
      let values = await store.rangeGet({ })
      t.deepEqual(values, [ { v: 1, id: 'a' }, { id: 'e', v: 5 } ], 'range read' )
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
