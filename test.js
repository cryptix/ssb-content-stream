const ssbServer = require('ssb-server')
const ssbConfig = require('ssb-config')
const pull = require('pull-stream')
const test = require('tape')
const { tmpdir } = require('os')
const ssbKeys = require('ssb-keys')

ssbServer
  .use(require('ssb-server/plugins/master'))
  .use(require('ssb-blobs'))
  .use(require('ssb-blob-content'))
  .use(require('./'))

const config = Object.assign(
  {},
  ssbConfig,
  {
    path: tmpdir(Math.random()),
    port: 40090,
    keys: ssbKeys.generate()
  }
)

const ssb = ssbServer(config)

const streamOpts = Object.assign( {}, ssb.whoami())

const collect = (t) => pull.collect((err, msgs) => {
  t.error(err, 'collect messages without error')
  t.equal(Array.isArray(msgs), true, 'messages is an array')
})

test('setup', (t) => {
  t.plan(3)

  ssb.blobContent.publish({ type: 'repeat', content: 'hello' }, (err, msg) => {
    t.error(err)
    ssb.blobContent.publish({ type: 'unique', content: 'world' }, (err, msg) => {
      t.error(err)
      ssb.blobContent.publish({ type: 'repeat', content: 'hello' }, (err, msg) => {
        t.error(err)
        t.end()
      })
    })
  })
})

test('createSource(opts) + createBlobHandler()', (t) => {
  t.plan(2)

  pull(
    ssb.contentStream.createSource(streamOpts),
    ssb.contentStream.createBlobHandler(),
    collect(t)
  )
})

test('createSource(opts) + createBlobHandler(cb)', (t) => {
  t.plan(4)

  pull(
    ssb.contentStream.createSource(streamOpts),
    ssb.contentStream.createBlobHandler((err, blobNum) => {
      console.log(blobNum)
      t.error(err, 'successful blob handler')
      t.equal(blobNum, 2)
    }),
    collect(t)
  )
})

test('createBlobHandlerSource(opts, cb)', (t) => {
  t.plan(2)

  pull(
    ssb.contentStream.createBlobHandlerSource(streamOpts),
    collect(t)
  )
})

test('createBlobHandlerSource(opts)', (t) => {
  t.plan(4)

  pull(
    ssb.contentStream.createBlobHandlerSource(streamOpts, (err, blobNum) => {
      t.error(err, 'successful blob handler')
      t.equal(blobNum, 2)
    }),
    collect(t)
  )
})

test.onFinish(ssb.close)

