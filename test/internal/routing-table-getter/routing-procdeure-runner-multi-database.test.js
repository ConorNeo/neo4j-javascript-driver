/**
 * Copyright (c) 2002-2020 "Neo4j,"
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

import MultiDatabaseRountingProcuderRunner from '../../../src/internal/routing-table-getter/routing-procedure-runner-multi-database'
import ServerAddress from '../../../src/internal/server-address'
import TxConfig from '../../../src/internal/tx-config'
import Result from '../../../src/result'
import FakeConnection from '../fake-connection'
import FakeSession from '../fake-session'

describe('#unit MultiDatabaseRountingProcuderRunner', () => {
  it('should run query over protocol with the correct params', () => {
    const initialAddress = ServerAddress.fromUrl('bolt://127.0.0.1')
    const bookmark = 'bookmark'
    const mode = 'READ'
    const database = 'adb'
    const sessionDatabase = 'session.database'
    const onComplete = () => 'nothing'
    const connection = new FakeConnection().withProtocolVersion(3)
    const context = { someContext: '1234' }
    const session = new FakeSession(null, connection)
      .withBookmark(bookmark)
      .withMode(mode)
      .withDatabase(sessionDatabase)
      .withOnComplete(onComplete)

    run({ connection, context, session, database, initialAddress })

    expect(connection.seenQueries).toEqual([
      'CALL dbms.routing.getRoutingTable($context, $database)'
    ])
    expect(connection.seenParameters).toEqual([
      { context: { ...context, address: initialAddress }, database }
    ])
    expect(connection.seenProtocolOptions).toEqual([
      {
        bookmark,
        txConfig: TxConfig.empty(),
        mode,
        database: sessionDatabase,
        afterComplete: onComplete
      }
    ])
  })

  it('should return a result', () => {
    const connection = new FakeConnection().withProtocolVersion(3)
    const context = { someContext: '1234' }
    const session = new FakeSession(null, connection)

    const result = run({ connection, context, session })

    expect(result).toEqual(jasmine.any(Result))
    expect(result._streamObserverPromise).toEqual(jasmine.any(Promise))
  })
})

function run ({
  connection,
  database = 'adb',
  context,
  session,
  initialAddress
}) {
  const runner = new MultiDatabaseRountingProcuderRunner(initialAddress)
  return runner.run(connection, database, context, session)
}