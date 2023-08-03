/**
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import neo4j from '../src'
import sharedNeo4j from './internal/shared-neo4j'
import { Neo4jTestContainer } from './internal/node/neo4j-test-container'

describe('#integration null value', async () => {
  it('should support null', await testValue(null))
})

describe('#integration floating point values', async () => {
  it('should support float 1.0 ', await testValue(1))
  it('should support float 0.0 ', await testValue(0.0))
  it('should support pretty big float ', await testValue(3.4028235e38)) // Max 32-bit
  it('should support really big float ', await testValue(1.7976931348623157e308)) // Max 64-bit
  it('should support pretty small float ', await testValue(1.4e-45)) // Min 32-bit
  it('should support really small float ', await testValue(4.9e-324)) // Min 64-bit
})

describe('#integration integer values', async () => {
  it(
    'should support integer larger than JS Numbers can model',
    await testValue(neo4j.int('0x7fffffffffffffff'))
  )
  it(
    'should support integer smaller than JS Numbers can model',
    await testValue(neo4j.int('0x8000000000000000'))
  )
})

describe('#integration boolean values', async () => {
  it('should support true ', await testValue(true))
})

describe('#integration string values', async () => {
  it('should support simple string ', await testValue('abcdefghijklmnopqrstuvwxyz'))
})

describe('#integration list values', async () => {
  it('should support empty lists ', await testValue([]))
  it('should support sparse lists ', await testValue([undefined, 4], [null, 4]))
  it('should support list lists ', await testValue([[], [1, 2, 3]]))
  it('should support map lists ', await testValue([{}, { a: 12 }]))
})

describe('#integration map values', async () => {
  it('should support empty maps ', await testValue({}))
  it(
    'should support basic maps ',
    await testValue({ a: 1, b: {}, c: [], d: { e: 1 } })
  )
})

describe('#integration node values', () => {
  it('should support returning nodes ', async done => {
    const boltUrl = (await Neo4jTestContainer.getInstance()).getBoltUrl()
    // Given
    const driver = neo4j.driver(
      boltUrl,
      sharedNeo4j.authToken
    )
    const session = driver.session()

    // When
    session
      .run("CREATE (n:User {name:'Lisa'}) RETURN n, id(n)")
      .then(result => {
        const node = result.records[0].get('n')

        expect(node.properties).toEqual({ name: 'Lisa' })
        expect(node.labels).toEqual(['User'])
        expect(node.identity).toEqual(result.records[0].get('id(n)'))
      })
      .then(() => driver.close())
      .then(() => done())
  })
})

describe('#integration relationship values', () => {
  it('should support returning relationships', async done => {
    const boltUrl = (await Neo4jTestContainer.getInstance()).getBoltUrl()

    // Given
    const driver = neo4j.driver(
      boltUrl,
      sharedNeo4j.authToken
    )
    const session = driver.session()

    // When
    session
      .run("CREATE ()-[r:User {name:'Lisa'}]->() RETURN r, id(r)")
      .then(result => {
        const rel = result.records[0].get('r')

        expect(rel.properties).toEqual({ name: 'Lisa' })
        expect(rel.type).toEqual('User')
        expect(rel.identity).toEqual(result.records[0].get('id(r)'))
      })
      .then(() => driver.close())
      .then(() => done())
  })
})

describe('#integration path values', () => {
  let container

  beforeAll( async () => {
    container = await Neo4jTestContainer.getInstance()
  })

  it('should support returning paths', done => {
    // Given
    const driver = neo4j.driver(
      container.getBoltUrl(),
      sharedNeo4j.authToken
    )
    const session = driver.session()

    // When
    session
      .run(
        "CREATE p=(:User { name:'Lisa' })<-[r:KNOWS {since:1234.0}]-() RETURN p"
      )
      .then(result => {
        const path = result.records[0].get('p')

        expect(path.start.properties).toEqual({ name: 'Lisa' })
        expect(path.end.properties).toEqual({})

        // Accessing path segments
        expect(path.length).toEqual(1)
        for (let i = 0; i < path.length; i++) {
          const segment = path.segments[i]
          // The direction of the path segment goes from lisa to the blank node
          expect(segment.start.properties).toEqual({ name: 'Lisa' })
          expect(segment.end.properties).toEqual({})
          // Which is the inverse of the relationship itself!
          expect(segment.relationship.properties).toEqual({ since: 1234 })
        }
      })
      .then(() => driver.close())
      .then(() => done())
      .catch(err => {
        console.log(err)
      })
  })
})

describe('#integration byte arrays', () => {

  it('should support returning empty byte array if server supports byte arrays', async done => {
    (await testValue(new Int8Array(0)))(done)
  }, 60000)

  it('should support returning empty byte array if server supports byte arrays', async done => {
    (await testValues([new Int8Array(0)]))(done)
  }, 60000)

  it('should support returning short byte arrays if server supports byte arrays', async done => {
    (await testValues(randomByteArrays(100, 1, 255)))(done)
  }, 60000)

  it('should support returning medium byte arrays if server supports byte arrays', async done => {
    (await testValues(randomByteArrays(50, 256, 65535)))(done)
  }, 60000)

  it('should support returning long byte arrays if server supports byte arrays', async done => {
    (await testValues(randomByteArrays(10, 65536, 2 * 65536)))(done)
  }, 60000)

  it('should fail to return byte array if server does not support byte arrays', async done => {
    const container = await Neo4jTestContainer.getInstance()

    const driver = neo4j.driver(
      container.getBoltUrl(),
      sharedNeo4j.authToken
    )
    const session = driver.session()
    session
      .run('RETURN $array', { array: randomByteArray(42) })
      .catch(error => {
        expect(error.message).toEqual(
          'Byte arrays are not supported by the database this driver is connected to'
        )
      })
      .then(() => driver.close())
      .then(() => done())
  }, 60000)
})

async function testValue (actual, expected) {
  const container = await Neo4jTestContainer.getInstance()

  return done => {
    const driver = neo4j.driver(
      container.getBoltUrl(),
      sharedNeo4j.authToken
    )
    const queryPromise = runReturnQuery(driver, actual, expected)

    queryPromise
      .then(() => driver.close())
      .then(() => done())
      .catch(error => done.fail(error))
  }
}

async function testValues (values) {
  const container = await Neo4jTestContainer.getInstance()

  return done => {
    const driver = neo4j.driver(
      container.getBoltUrl(),
      sharedNeo4j.authToken
    )
    const queriesPromise = values.reduce(
      (acc, value) => acc.then(() => runReturnQuery(driver, value)),
      Promise.resolve()
    )

    queriesPromise
      .then(() => driver.close())
      .then(() => done())
      .catch(error => done.fail(error))
  }
}

function runReturnQuery (driver, actual, expected) {
  const session = driver.session()
  return new Promise((resolve, reject) => {
    session
      .run('RETURN $val as v', { val: actual })
      .then(result => {
        expect(result.records[0].get('v')).toEqual(expected || actual)
      })
      .then(() => session.close())
      .then(() => {
        resolve()
      })
      .catch(error => {
        reject(error)
      })
  })
}

function randomByteArrays (count, minLength, maxLength) {
  return range(count).map(() => {
    const length = random(minLength, maxLength)
    return randomByteArray(length)
  })
}

function randomByteArray (length) {
  const array = range(length).map(() => random(-128, 127))
  return new Int8Array(array)
}

function range (size) {
  const arr = []
  for (let i; i < size; i++) {
    arr.push(i)
  }
  return arr
}

function random (lower, upper) {
  const interval = upper - lower
  return lower + Math.floor(Math.random() * interval)
}
