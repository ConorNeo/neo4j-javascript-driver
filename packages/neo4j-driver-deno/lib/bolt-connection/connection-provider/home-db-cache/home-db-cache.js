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

const DEFAULT_KEY = -1

export default class HomeDBCache {
  constructor ({ maxHomeDatabaseDelay }) {
    this._disabled = maxHomeDatabaseDelay === 0
    this._maxHomeDatabaseDelay = maxHomeDatabaseDelay || 5000
    this._cache = new Map()
  }

  set ({ impersonatedUser, auth, databaseName }) {
    if (this._disabled) {
      return null
    }
    if (databaseName == null) {
      return null
    }

    if (this._maxHomeDatabaseDelay > 0) {
      let key = impersonatedUser || auth

      if (key == null) {
        key = DEFAULT_KEY // This is for when auth is turned off basically
      }

      this._cache.set(key, { databaseName: databaseName, insertTime: Date.now() })
    }
  }

  get ({ impersonatedUser, auth }) {
    if (this._disabled) {
      return null
    }
    let key = impersonatedUser || auth
    if (key == null) {
      key = DEFAULT_KEY // This is for when auth is turned off basically
    }

    const dbAndCreatedTime = this._cache.get(key)

    if (dbAndCreatedTime == null) {
      return null
    }

    if (Date.now() > dbAndCreatedTime.insertTime + this._maxHomeDatabaseDelay) {
      this._cache.delete(key)
      return null
    } else {
      return this._cache.get(key).databaseName
    }
  }

  clearCache () {
    this._cache = new Map()
  }
}
