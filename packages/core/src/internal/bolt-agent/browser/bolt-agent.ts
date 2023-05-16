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
/* eslint-disable */
// @ts-ignore: browser code so must be skipped by ts
export function fromVersion (version: string, windowProvider = () => window): string {
  // @ts-ignore: browser code so must be skipped by ts
  const APP_VERSION = windowProvider().navigator.appVersion

  //APP_VERSION looks like 5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36
  const OS = APP_VERSION.split("(")[1].split(")")[0];

  return `neo4j-javascript/${version} (${OS})`
}
/* eslint-enable */