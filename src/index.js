const {castArray, isString, isArray} = require('lodash')
const {Either} = require('monet');

/**
 *
 * @param avro
 * @return {Either<string, {}>}
 */
module.exports = (avro) => {
  return convertType({}, normalizeFieldType(avro))
    .map(({definitions, converted}) => ({
        "definitions": definitions,
        "$schema": "http://json-schema.org/draft-07/schema#",
        ...converted
      }
    ))
}

/**
 *
 * @param avro
 * @return {Either<string, {definitions: {}, converted: {}}>}
 */
const convertTypeRecord = (definitions, avro) => {
  if (!(avro.fields && isArray(avro.fields))) {
    return Either.left('must have a .fields property of type array ')
  } else
    return avro.fields
      .reduce((accE, field) => accE.flatMap(({definitions: accDefinitions, converted: accConverted}) =>
        convertField(accDefinitions, field).bimap(
          err => `.fields[@name='${field.name}'] : ${err}`,
          ({definitions, converted}) => ({definitions, converted: {...accConverted, [field.name]: converted}})
        )
      ), Either.right({definitions, converted: {}}))
      .map(({definitions: accDefinitions, converted: accConverted}) => ({
        definitions: accDefinitions,
        converted: {
          type: "object",
          required: avro.fields
            .filter(field => !fieldIsNullable(field))
            .map(({name}) => name),
          properties: accConverted
        }
      }));
}

/**
 *
 * @param field
 * @return {Either<string, {definitions: {}, converted: {}}>}
 */
const convertField = (definitions, field) => {
  const types = normalizeFieldTypes(field.type)

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

  return convertType(definitions, types[typeIndex])
    .leftMap(err => `.type[${typeIndex}] : ${err}`)
}

/**
 *
 * @param type
 * @return {Either<string, {definitions: {}, converted: {}}>}
 */
const convertType = (definitions, type) => {
  const {type: tt, logicalType} = type
  let jsonType;
  if (!!logicalType) {
    return Either.left(`.logicalType is not supported`)
  } else if (tt === 'null') {
    // should not occurs
  } else if (tt === 'boolean') {
    jsonType = {type: "boolean"}
  } else if (tt === 'int') {
    jsonType = {type: "integer"}
  } else if (tt === 'long') {
    jsonType = {type: "integer", format: "int64"}
  } else if (tt === 'float') {
    jsonType = {type: "number"}
  } else if (tt === 'double') {
    jsonType = {type: "number"}
  } else if (tt === 'bytes') {
    jsonType = {type: "integer"}
  } else if (tt === 'string') {
    jsonType = {type: "string"}
  } else if (tt === 'record') {
    return convertTypeRecord(definitions, type)
      .map(({definitions, converted}) => {
        if (type.name) {
          if (definitions[type.name]) {
            return Either.left(`a record named ${type.name} is already defined in this schema`)
          } else {
            return {
              definitions: {
                ...definitions,
                [type.name]: converted
              },
              converted: {"$ref": "#/definitions/" + type.name}
            }
          }
        } else {
          return {definitions, converted}
        }
      })
  } else if (tt === 'enum') {
    // not yet supported
  } else if (tt === 'array') {
    return convertType(definitions, normalizeFieldType(type.items)).bimap(
      err => `.items : ${err}`,
      ({definitions, converted}) => ({definitions, converted: {type: "array", items: converted}})
    )
  } else if (tt === 'fixed') {
    // not yet supported
  } else if (definitions[tt]) { //it's a ref to an existing type
    jsonType = {"$ref": "#/definitions/" + tt}
  }

  if (jsonType) {
    return Either.right({definitions, converted: jsonType})
  } else {
    return Either.left(`.type [${tt}] is not supported`)
  }
}

const normalizeFieldTypes = (types) => ((types && castArray(types)) || [])
  .map(normalizeFieldType)

const normalizeFieldType = (type) => isString(type) ? {type} : type

const fieldIsNullable = (field) => !!normalizeFieldTypes(field.type).find(({type}) => type === 'null')
