const pkg = require('./package.json')
const pull = require('pull-stream')
const pullCat = require('pull-cat')
const ssbRef = require('ssb-ref')
const pullCatch = require('pull-catch')
const pullTee = require('pull-tee')
const debug = require('debug')('ssb-content-stream')

const MB = 1024 * 1024
const MAX_SIZE = 5 * MB
const noop = () => {}

// https://github.com/ssbc/ssb-backlinks/blob/master/emit-links.js#L47-L55
function walk (obj, fn) {
  if (obj && typeof obj === 'object') {
    for (var k in obj) {
      walk(obj[k], fn)
    }
  } else {
    fn(obj)
  }
}

exports.init = (ssb) => {
  const contentStream = {
    createSource: (opts) => pullCat([
      pull(
        ssb.createHistoryStream(opts),
        pull.map(msg => {
          // get blobs referenced by message
          // TODO: use ssb-backlinks or something to cache
          const blobs = []
          walk(msg, (val) => {
            if (ssbRef.isBlob(val)) {
              blobs.push(val)
            }
          })
          debug('referenced blobs: %O', blobs)

          return blobs
        }),
        pull.flatten(), // items of arrays of strings => items of strings
        pull.unique(), // don't get the same blob twice
        pull.map((hash) => {
          debug('getting blob: %s', hash)
          return pull(
            ssb.blobs.get({
              hash,
              max: opts.max || MAX_SIZE
            }),
            pullCatch((err) => {
              debug('harmless error pulling blob: %O', err)
            }) // disregard blobs that don't exist or are too big
          )
        }),
        pull.flatten() // convert from pull-stream source to items
      ),
      ssb.createHistoryStream(opts)
    ]),
    createBlobHandler: (cb = noop) => {
      let count = 0
      const errors = []

      return pull(
        pullTee(
          pull(
            pull.filter(Buffer.isBuffer),
            pull.drain((blob) => {
              pull(
                pull.values([blob]),
                ssb.blobs.add((err) => {
                  if (err) errors.push(err)
                  count += 1
                })
              )
            }, (err) => {
              if (err || errors.length) {
                const errConcat = [err, errors]
                return cb(errConcat, count)
              } else {
                cb(null, count)
              }
            })
          )
        ),
        pull.filterNot(Buffer.isBuffer),
      )
    },
    createBlobHandlerSource: (opts, cb) => pull(
      contentStream.createSource(opts),
      contentStream.createBlobHandler(cb)
    ),
    createSink: (opts, cb) => {
      // In a perfect world this would probably look like:
      //
      // 1. Receive all messages
      // 2. Extract blob references
      // 3. Save each message to database to validate
      // 4. Receive all blobs
      // 5. Hash each blob
      // 6. Compare against extracted blob references and save blobs we want
      //
      // Unfortunately in the case of off-chain content we don't want to be in
      // a position where we're saving a message before we have the blob in the
      // blob store. This means that we need to save the blobs before we validate
      // the message, which leads us to something that looks more like:
      //
      // 1. Receive all messages
      // 2. Extract blob references
      // 3. Receive all blobs
      // 4. Hash each blob
      // 5. Compare against extracted blob references to save blobs we want
      // 6. Start listening for database errors
      // 7. Save each message to database to validate
      // 8. If a message is invalid, delete the uniquely referenced blob

      const extractedBlobs = {
        // '&a': [ '%b', '%c']
      }

      ssb.on('invalid', (err, msg) => {
        // TODO test to make sure this actually emits on invalid messages
        console.log(msg.key, err)
      })

      //      return ssb.createWriteStream((err) => 
      // )
    }
  }

  return contentStream
}

exports.manifest = {
  createSource: 'source',
  createBlobHandler: 'source'
}

exports.permissions = [ 'createSource' ]
exports.name = pkg.name.replace('ssb-', '')
exports.version = pkg.version
