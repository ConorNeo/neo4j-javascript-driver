import { GenericContainer, Wait } from 'testcontainers'
import { DockerImageName } from 'testcontainers/dist/docker-image-name'

class _PrivateNeo4jTestContainer {
  containerLogs = false //will be used later
  version = "5.8"

  constructor () {}

  async start(){
    console.log("Starting container")

    this.container = await new GenericContainer(new DockerImageName(null, "neo4j", `${this.version}-enterprise`).toString())
      .withEnv("NEO4J_AUTH", "neo4j/password")
      .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
      .withExposedPorts(7687, 7474)
      .withWaitStrategy(Wait.forLogMessage(new RegExp("Started")))
      .start()

    console.log("Container started")

    if(this.containerLogs){
      const stream = await this.container.logs();
      stream
        .on("data", line => console.log(line))
        .on("err", line => console.error(line))
        .on("end", () => console.log("Stream closed"));
    }
  }

  getHost(){
    if(this.container){
      return this.container.getHost()
    }

    throw Error("Container not started")
  }

  getBoltPort(){
    if(this.container){
      return this.container.getMappedPort(7687)
    }

    throw Error("Container not started")
  }

  getHttpPort(){
    if(this.container){
      return this.container.getMappedPort(7474)
    }

    throw Error("Container not started")
  }

  getBoltUrl(){
    return `bolt://${this.getHost()}:${this.getBoltPort()}`
  }

  getHttpUrl(){
    return `bolt://${this.getHost()}:${this.getHttpPort()}`
  }

  stop(){
    //this.container.stop()
  }
}

export class Neo4jTestContainer {
  constructor () {
    throw new Error("use getInstance()")
  }

  static async getInstance(){
    if(!Neo4jTestContainer.instance){
      Neo4jTestContainer.instance = new _PrivateNeo4jTestContainer()
      await Neo4jTestContainer.instance.start()
    }
    return Neo4jTestContainer.instance
  }
}