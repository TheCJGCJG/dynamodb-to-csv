import { dbScan } from './db'
import _ from 'lodash'
import async from 'async'
import config from '../config.json'
import fs from 'fs'
import toCSV from 'array-to-csv'

export const handler = () => {
  async.map(config.tables, async (table, done) => {
    const {
      name,
      output_filename,
      format
    } = table

    try {
      const entries = await getDatabaseEntries(name)
      const data = generateDataStructures(entries)
      const output = generateOutput(format, data)
      saveOutput(output_filename, output)
      // console.log(output)
    } catch (error) {
      done(error)
    }
  }, (error, outputs) => {
    console.log(error, outputs)
  })
}


const generateOutput = (format, data) => {
  let output
  switch (format) {
    case 'CSV':
      output = []

      output.push(_.map(data, (value, key) => key))

      // console.log()
      _.times(data[Object.keys(data)[0]].length, (i) => {
        output.push(_.map(data, (value, key) => {
          // console.log(toCSV(_.get(value, i)))
          return _.get(value, i)
        }))
      })

      return toCSV(output)

      break
    case 'JSON':

      break
    default:
      break
  }

  return output

}


const saveOutput = (filename, data) => {
  const outFile = fs.createWriteStream(filename, { encoding: 'utf16le' })
  outFile.write(data)
  outFile.end()
}

const generateDataStructures = (entries) => {
  let headers = []
  let data = []
  let output = {}

  _.map(entries, (value) => {
    _.map(entries, (firstEntry, firstKey) => {
        _.map(firstEntry, (secondEntry, secondKey) => {
          if (typeof secondEntry === 'object' || typeof secondEntry === 'array') {
            _.map(secondEntry, (thirdEntry, thirdKey) => {
              if (!headers.includes(`${secondKey}.${thirdKey}`)) {
                headers.push(`${secondKey}.${thirdKey}`)
              }
            })
          } else {
            if (!headers.includes(secondKey)) {
              headers.push(secondKey)
            }
          }
        })
    })
  })

    _.map(headers, (path) => {
      output[path] = []
    })

  _.forEach(entries, (entry) => {
    _.map(headers, (path) => {
      output[path].push(_.get(entry, path) || '')
    })
  })

  return output
}

const getDatabaseEntries = (name) =>
  new Promise((resolve, reject) => {

  let lastEvaluated

  async.doWhilst(async (done) => {
    try {
      const {
        Items,
        LastEvaluatedKey
      } = await dbScan(name, {
        ExclusiveStartKey: lastEvaluated
      })

      lastEvaluated = LastEvaluatedKey
      done(null, Items)
    } catch (error) {
      done(error)
    }
  }, () => !!lastEvaluated, (error, items) => {
    if (error) {
      reject(error)
    } else {
      resolve(items)
    }
  })
})
