const {castArray, isString, isArray} = require('lodash')
const {Either} = require('monet');

/**
 *
 * @param avro
 * @return {Either<string, {}>}
 */
module.exports = (avro) => {
  return convertTypeRecord(avro)
    .map(convertedObject => ({
        "definitions": {},
        "$schema": "http://json-schema.org/draft-07/schema#",
        ...convertedObject
      }
    ))
}

/**
 *
 * @param avro
 * @return {Either<string, Object>}
 */
const convertTypeRecord = (avro) => {
  if (!(avro.fields && isArray(avro.fields))) {
    return Either.left('must have a .fields property of type array ')
  } else
    return avro.fields
      .reduce((accE, field) => accE.flatMap(acc =>
        convertField(field).bimap(
          err => `.fields[@name='${field.name}'] : ${err}`,
          convertedField => ({...acc, [field.name]: convertedField})
        )
      ), Either.right({}))
      .map(convertedFields => ({
        type: "object",
        required: avro.fields
          .filter(field => !fieldIsNullable(field))
          .map(({name}) => name),
        properties: convertedFields
      }));
}

/**
 *
 * @param field
 * @return {Either<string, Object>}
 */
const convertField = (field) => {
  const types = normalizeFieldType(field.type)

  let typeIndex;
  if (fieldIsNullable(field) && types.length === 2) {
    const nullableTypeIndex = types.findIndex(({type: typeName}) => typeName === 'null')
    typeIndex = (nullableTypeIndex + 1) % 2;
    if (types[typeIndex].type === 'null') {
      return Either.left(`must not have 2 .type of type ["null"]`)
    }
  } else if (types.length === 1) {
    typeIndex = 0
  } else {
    return Either.left(`must have exactly 1 .type or 2 if one of them is ["null"]`)
  }

  return convertType(types[typeIndex])
    .leftMap(err => `.type[${typeIndex}] : ${err}`)
}

/**
 *
 * @param type
 * @return {Either<string, Object>}
 */
const convertType = (type) => {
  const {type: tt, logicalType} = type
  if (!!logicalType) {
    return Either.left(`.logicalType is not supported`)
  } else if (tt === 'boolean') {
    return Either.right({type: "boolean"})
  } else if (tt === 'int') {
    return Either.right({type: "integer"})
  } else if (tt === 'bytes') {
    return Either.right({type: "integer"})
  } else if (tt === 'long') {
    return Either.right({type: "integer", format: "int64"})
  } else if (tt === 'float') {
    return Either.right({type: "number"})
  } else if (tt === 'double') {
    return Either.right({type: "number"})
  } else if (tt === 'string') {
    return Either.right({type: "string"})
  } else if (tt === 'record') {
    return convertTypeRecord(type)
  } else if (tt === 'array') {
    return convertType(type.items).bimap(
      err => `.items : ${err}`,
      convertedItem => ({type: "array", items: convertedItem})
    )
  } else {
    return Either.left(`.type [${tt}] is not supported`)
  }
}

const normalizeFieldType = (type) => ((type && castArray(type)) || [])
  .map(oneType => isString(oneType) ? {type: oneType} : oneType)

const fieldIsNullable = (field) => !!normalizeFieldType(field.type).find(({type}) => type === 'null')
