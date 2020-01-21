import avro2jsons from '../src/index'
import * as assert from 'assert';

describe('avro2jsons()',  () => {
  it('should convert avro to json schema', () => {
    const avro = {
      "name": "com.test.Order",
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
              "name": "com.test.OrderLine",
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
});
