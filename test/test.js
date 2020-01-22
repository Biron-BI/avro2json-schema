const avro2jsons = require('../src/index')
const assert = require('assert');

describe('avro2jsons()',  () => {
  it('should convert avro to json schema', () => {
    const avro = {
      "type": "record",
      "fields": [
        {
          "name": "id",
          "type": "int"
        },
        {
          "name": "is_valid",
          "type": "boolean"
        },
        {
          "name": "type",
          "type": "string"
        },
        {
          "name": "total",
          "type": "bytes"
        },
        {
          "name": "orderlines",
          "type": {
            "type": "array",
            "items": {
              "type": "record",
              "fields": [
                {
                  "name": "id",
                  "type": "long"
                },
                {
                  "name": "f",
                  "type": "float"
                },
                {
                  "name": "d",
                  "type": ["null", "double"]
                }
              ]
            }
          }
        }
      ]
    }
    const expected = {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "definitions": {},
      "type": "object",
      "required": ["id", "is_valid", "type", "total", "orderlines"],
      "properties": {
        "id": {
          "type": "integer"
        },
        "is_valid": {
          "type": "boolean"
        },
        "type": {
          "type": "string"
        },
        "total": {
          "type": "integer"
        },
        "orderlines": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["id", "f"],
            "properties": {
              "id": {
                "type": "integer",
                "format": "int64"
              },
              "f": {
                "type": "number"
              },
              "d": {
                "type": "number"
              }
            }
          }
        }
      }
    }
    assert.deepEqual(avro2jsons(avro).bimap(err => {throw new Error(err)}, v=>v).right(), expected);
  });

  it('should convert array of string', () => {
    const avro = {
      "type": "record",
      "fields": [
        {
          "name": "orderlines",
          "type": {
            "type": "array",
            "items": "string"
          }
        }
      ]
    }
    const expected = {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "definitions": {},
      "type": "object",
      "required": ["orderlines"],
      "properties": {
        "orderlines": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
    assert.deepEqual(avro2jsons(avro).bimap(err => {throw new Error(err)}, v=>v).right(), expected);
  });

  it('should convert ref of declared record', () => {
    const avro = {
      "name": "com.Foo",
      "type": "record",
      "fields": [
        {
          "name": "address1",
          "type": {
            "type": "record",
            "name": "Address",
            "fields": [
              {
                "name" : "inline",
                "type":"string"
              }
            ]
          }
        },
        {
          "name": "address2",
          "type": "Address"
        }
      ]
    }
    const expected = {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "definitions": {
        "Address": {
          "type": "object",
          "required": ["inline"],
          "properties": {
            "inline": {
              "type": "string"
            }
          }
        },
        "com.Foo": {
          "type": "object",
          "required": ["address1", "address2"],
          "properties": {
            "address1": {
              "$ref": "#/definitions/Address"
            },
            "address2": {
              "$ref": "#/definitions/Address"
            }
          }
        }
      },
      "$ref": "#/definitions/com.Foo"
    }
    assert.deepEqual(avro2jsons(avro).bimap(err => {throw new Error(err)}, v=>v).right(), expected);
  });

  it('should convert basic type', () => {
    const avro = "string"
    const expected = {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "definitions": {},
      "type": "string"
    }
    assert.deepEqual(avro2jsons(avro).bimap(err => {throw new Error(err)}, v=>v).right(), expected);
  });
});
