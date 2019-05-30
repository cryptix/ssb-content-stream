const pkg = require('./package.json')
const pull = require('pull-stream')
const pullDrain = require('pull-stream/sinks/drain')
const pullCat = require('pull-cat')
const pCont = require('pull-cont/source')
const crypto = require('crypto')
const level = require('level')
const path = require('path')
const lodash = require('lodash')

// i would drop the .box (since it's not encrypted)
// but doing so would requier a breaking change to ssb-validate
const contentSuffix = '.box.offchain.sha256'

const isContentMessage = (msg) => {
  if (msg === null) {
    return false
  }

  // Make sure `content` is an object.
  if (typeof msg.value.content !== 'string') {
    return false
  }

  // Make sure this is a content message.
  if (msg.value.content.endsWith(contentSuffix) === false) {
    return false
  }

  return true
}

const getContentHref = (msg) =>
  isContentMessage(msg) ? msg.value.content : null

const createHref = (str) => {
  const hash = crypto.createHash('sha256').update(str)
  const digest = hash.digest('base64')
  return digest + contentSuffix
}

exports.init = (ssb, config) => {
  var db = level(path.join(config.path, 'content'))

  const contentStream = {
    // Returns `createHistoryStream()` with off-chain content added inline.
    //
    // Items that pass the pattern are pulled from the database and prepended
    // to the stream so that they don't bottleneck the indexing process.
    // cryptix: I don't think this will perform well for replication. we can't buffer that many messages.
    // It would be besser to transfer them as [meta:content] pairs and process them directly
    createSource: (opts) => pullCat([
      pull(
        ssb.createHistoryStream(opts),
        pull.map(getContentHref),
        pull.unique(),
        pull.asyncMap(db.get)
      ),
      ssb.createHistoryStream(opts)
    ]),

    // Consumes the output of `createSource()` by receiving messages + content
    // and adding them to the database for indexing. This method can tell the
    // difference between messages and content because messages are objects
    // and content is a JSON string.
    //
    // This is a slightly funky implementation because any part of the process
    // could throw an error, so we keep an `errors` array that helps us keep
    // track of all of the moving parts that could crash and burn.
    createHandler: () => {
      const contentMap = {}

      return pull(
        pull.map(val => {
          console.log(val)
          // If the item is a content string we add it to the `contentMap`
          // object for processing later.
          if (typeof val === 'string') {
            const href = createHref(val)
            contentMap[href] = val
            return null
          }

          return val
        }),
        pull.through((msg) => {
          // Now we take each message and check whether it's in `contentMap`.
          const href = getContentHref(msg)

          if (href == null) {
            return
          }

          if (contentMap[href] == null) {
            throw new Error('messages from content stream must contain content')
          }

          db.put(href, contentMap[href], (err) => {
            if (err) throw err
          })
        })
      )
    },
    // Convenience function for taking a Scuttlebutt message and squeezing out
    // the off-chain content for consumption by applications. This is used
    // with `ssb.addMap()` so that views see content by default, but it may
    // become useful in other contexts.
    getContent: (msg, cb) => {
      if (isContentMessage(msg) === false) {
        return cb(null, msg)
      }

      db.get(msg.value.content, (err, val) => {
        if (err) return cb(err, msg)

        lodash.set(msg, 'value.meta.original.content', msg.value.content)
        msg.value.content = JSON.parse(val)
        cb(null, msg)
      })
    },

    // Same API as calling `createHistoryStream()` except that it returns the
    // off-chain content instead of just the metadata.
    getContentStream: (opts) => pull(
      contentStream.createSource(opts),
      contentStream.createHandler(),
      pull.asyncMap(contentStream.getContent)
    ),

    // Takes message value and posts it to your feed as off-chain content.
    publish: (content, cb) => {
      const str = JSON.stringify(content)
      const href = createHref(str)
      
      db.put(href, str, (err) => {
        if (err) return cb(err)
        // might want to wrap ssb.add and somehow trigger indexing of content that way?!
        // no, add is a even more low-level.
        // publish is a wrapper around ssb-feed which adds author and the next timestamp, so: no.
        // i think what we need is another unboxer-like map?!
        // but i'm not sure we can influence oder?
        //  which seems silly since it would be important if this is supposed to be generic
        ssb.publish(href, cb)
      })
    }
  }

  // error if offchain to prompt use of contentStream?!
  // ssb.createHistoryStream.hook((fn, args) => {
  //   var self = this
  //   return pCont(function (cb) {
  //     pull(
  //       // pluck the first message of the feed to see if it is
  //       fn.apply(self, {id:args.id, limit:1}),
  //       pullDrain((msg) => {
  //         if (isContentMessage(msg)) {
  //           cb(new Error("content-stream: use appropriate handler"))
  //           return
  //         }
  //         // pass through if it isnt
  //         cb(null, pull(
  //           fn.apply(self, args)
  //         ))
  //       })  
  //     )
  //   })
  // })


  // XXX: is this a bad idea? we want to keep clients from publishing inline content
  // cryptix: maybe. for one, it breaks indexing of plugins like friends
  const originalPublish = ssb.publish
  // ssb.publish = contentStream.publish

  // This only works when queries are started when `{ private: true }`
  ssb.addMap(contentStream.getContent)

  return contentStream
}

exports.manifest = {
  publish: 'async',
  createSource: 'source',
  getContentStream: 'source'
}

exports.permissions = [ 'createSource' ]
exports.name = pkg.name.replace('ssb-', '')
exports.version = pkg.version
