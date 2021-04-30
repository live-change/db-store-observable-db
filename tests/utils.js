function prepareDatabaseTest(t, connection, dbName, storeName) {
  t.test('prepare database', async t => {
    t.plan(2)
    t.test('create database if not-exists', async t => {
      try {
        t.plan(1)
        await connection.deleteDatabase(dbName).catch(err=> console.log("DATABASE NOT EXISTS!"))
        console.log("CREATING")
        await connection.createDatabase(dbName, {})
        t.pass('created')
      } catch(e) {
        t.fail(e)
      }
    })
    t.test('create store', async t => {
      try {
        t.plan(1)
        await connection.createStore(dbName, storeName)
        t.pass('created')
      } catch(e) {
        t.fail(e)
      }
    })
  })
}

module.exports = { prepareDatabaseTest }