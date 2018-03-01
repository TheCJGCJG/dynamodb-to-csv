import config from '../config.json'
import { DynamoDB } from 'aws-sdk'

const doc = new DynamoDB.DocumentClient(config.aws)

const dbResponseHandler = (resolve, reject) => (error, data) => {
  if (error) {
    reject(error)
  } else {
    resolve(data)
  }
}

export const dbScan = (TableName, { FilterExpression, ExpressionAttributeValues, Limit, ExclusiveStartKey }) =>
  new Promise((resolve, reject) => {
    doc.scan({
      FilterExpression,
      ExpressionAttributeValues,
      TableName,
      ExclusiveStartKey,
      Limit
    }, dbResponseHandler(resolve, reject))
  })
